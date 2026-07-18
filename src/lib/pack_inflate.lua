-- inflate.lua
-- Zlib inflate/deflate wrappers and the binary-search stream-boundary finder.
--
-- Prefers a pure-Lua cpu_deflate implementation (libdeflate / deflate.lua) and
-- falls back to the OpenComputers data component when it is unavailable.

local component = require("component")
local data_comp = component.data

local D = require("debug")
local dbg         = D.dbg
local dbg_hex     = D.dbg_hex

local M = {}

--------------------------------------------------------------------------------
-- Optional pure-Lua zlib inflater
--
-- The referenced libdeflate/deflate.lua exposes DEFLATE.inflate_zlib.
-- We prefer that CPU-based implementation, and only fall back to the
-- component-based path if the module is unavailable.
--------------------------------------------------------------------------------
M.cpu_deflate = nil

do
  local ok, mod = pcall(require, "deflate")
  if ok and type(mod) == "table" and type(mod.inflate_zlib) == "function" then
    M.cpu_deflate = mod
    dbg("cpu_deflate: pure-Lua deflate module loaded successfully")
  else
    if not ok then
      dbg("cpu_deflate: failed to load deflate module (%s) – will use data component", tostring(mod))
    else
      dbg("cpu_deflate: module loaded but inflate_zlib not found (type=%s)", type(mod))
    end
  end
end

--------------------------------------------------------------------------------
-- silent_inflate_zlib
-- Calls cpu_deflate.inflate_zlib with stdout suppressed for the duration.
-- The third-party library prints "trailing garbage ignored" and similar
-- diagnostics directly via io.write/print; we redirect those to dbg so they
-- don't pollute the OC console.
--------------------------------------------------------------------------------
local function silent_inflate_zlib(opts)
  local captured = {}
  local orig_print    = print
  local orig_io_write = io.write

  -- Redirect both print() and io.write() used by the library
  print = function(...)
    local parts = {}
    for i = 1, select("#", ...) do
      parts[i] = tostring(select(i, ...))
    end
    captured[#captured + 1] = table.concat(parts, "\t")
  end
  io.write = function(...)
    local parts = {}
    for i = 1, select("#", ...) do
      parts[i] = tostring(select(i, ...))
    end
    captured[#captured + 1] = table.concat(parts)
  end

  local ok, err = pcall(M.cpu_deflate.inflate_zlib, opts)

  print    = orig_print
  io.write = orig_io_write

  if not ok then error(err, 2) end
end

--------------------------------------------------------------------------------
-- cpu_inflate_zlib_full  (cpu path, whole-buffer)
--
-- Unlike inflate_zlib_slice (which bounds its input to expected_size + slack
-- because it's hunting for a stream boundary inside a larger packfile),
-- safe_inflate is handed an exact, already-delimited zlib buffer with no
-- known "expected size" ahead of time. So we just feed the whole thing to
-- the pure-Lua inflater and collect everything it produces.
--------------------------------------------------------------------------------
local function cpu_inflate_zlib_full(bytes)
  local CHUNK_SIZE = 8192
  local chunks, buf, buf_n, total_len = {}, {}, 0, 0

  local ok, err = pcall(function()
    silent_inflate_zlib({
      input  = bytes,
      output = function(byte)
        buf_n      = buf_n + 1
        buf[buf_n] = string.char(byte)
        total_len  = total_len + 1
        if buf_n >= CHUNK_SIZE then
          chunks[#chunks + 1] = table.concat(buf, "", 1, buf_n)
          buf_n = 0
        end
      end,
    })
  end)

  if not ok then
    error("cpu_inflate_zlib_full failed: " .. tostring(err))
  end

  if buf_n > 0 then
    chunks[#chunks + 1] = table.concat(buf, "", 1, buf_n)
  end

  dbg("cpu_inflate_zlib_full: inflated %d bytes via pure-Lua inflater", total_len)
  return table.concat(chunks)
end

--------------------------------------------------------------------------------
-- Pure-Lua DEFLATE compressor (RFC 1951 / RFC 1950)
--
-- The optional third-party `deflate` module only exposes inflate_zlib, so
-- there's no CPU-based deflate to fall back on for compression. This is a
-- small self-contained implementation (LZ77 hash-chain matching + fixed
-- Huffman coding, RFC 1951 §3.2.6) that needs no external dependency at all,
-- used as the energy-free fallback for M.safe_deflate.
--------------------------------------------------------------------------------
do
  local MOD_ADLER = 65521

  local function adler32(data)
    local a, b = 1, 0
    local len = #data
    local i = 1
    while i <= len do
      local chunk_end = math.min(i + 5551, len)
      for j = i, chunk_end do
        a = a + data:byte(j)
        b = b + a
      end
      a = a % MOD_ADLER
      b = b % MOD_ADLER
      i = chunk_end + 1
    end
    return (b * 65536 + a) % 4294967296
  end

  -- ── Bit writer ──────────────────────────────────────────────────────────
  -- DEFLATE packs ordinary fields LSB-first into the byte stream, but
  -- Huffman codes are transmitted MSB-first. Both funnel through the same
  -- per-bit push; they just pull bits out of the value in a different order.
  local function new_bitwriter()
    return { bytes = {}, cur = 0, nbits = 0 }
  end

  local function bw_push_bit(bw, bit)
    bw.cur = bw.cur | (bit << bw.nbits)
    bw.nbits = bw.nbits + 1
    if bw.nbits == 8 then
      bw.bytes[#bw.bytes + 1] = bw.cur
      bw.cur = 0
      bw.nbits = 0
    end
  end

  local function bw_write_lsb(bw, value, nbits)
    for i = 0, nbits - 1 do
      bw_push_bit(bw, (value >> i) & 1)
    end
  end

  local function bw_write_msb(bw, code, nbits)
    for i = nbits - 1, 0, -1 do
      bw_push_bit(bw, (code >> i) & 1)
    end
  end

  local function bw_tostring(bw)
    if bw.nbits > 0 then
      bw.bytes[#bw.bytes + 1] = bw.cur
      bw.cur, bw.nbits = 0, 0
    end
    local out, CH, n = {}, 4096, #bw.bytes
    for i = 1, n, CH do
      local j = math.min(i + CH - 1, n)
      out[#out + 1] = string.char(table.unpack(bw.bytes, i, j))
    end
    return table.concat(out)
  end

  -- ── Fixed Huffman code tables (RFC 1951 §3.2.6) ─────────────────────────
  local function fixed_lit_code(sym)
    if sym <= 143 then
      return 0x30 + sym, 8
    elseif sym <= 255 then
      return 0x190 + (sym - 144), 9
    elseif sym <= 279 then
      return 0x0 + (sym - 256), 7
    else
      return 0xC0 + (sym - 280), 8
    end
  end

  local function fixed_dist_code(sym)
    return sym, 5   -- fixed distance codes are always 5 bits, code == symbol
  end

  -- length (3..258) -> {sym, extra_bits, base_len}, precomputed per length
  local LEN_SYM_BASE = {}
  do
    local LENGTH_TABLE = {
      {3,0},{4,0},{5,0},{6,0},{7,0},{8,0},{9,0},{10,0},
      {11,1},{13,1},{15,1},{17,1},
      {19,2},{23,2},{27,2},{31,2},
      {35,3},{43,3},{51,3},{59,3},
      {67,4},{83,4},{99,4},{115,4},
      {131,5},{163,5},{195,5},{227,5},
      {258,0},
    }
    local sym = 257
    for _, entry in ipairs(LENGTH_TABLE) do
      local base, extra = entry[1], entry[2]
      local count = (base == 258) and 1 or (1 << extra)
      for v = base, base + count - 1 do
        LEN_SYM_BASE[v] = { sym, extra, base }
      end
      sym = sym + 1
    end
  end

  -- distance (1..32768) -> (sym, extra_bits, base), found via binary search
  local DIST_TABLE = {
    {1,0},{2,0},{3,0},{4,0},
    {5,1},{7,1},
    {9,2},{13,2},
    {17,3},{25,3},
    {33,4},{49,4},
    {65,5},{97,5},
    {129,6},{193,6},
    {257,7},{385,7},
    {513,8},{769,8},
    {1025,9},{1537,9},
    {2049,10},{3073,10},
    {4097,11},{6145,11},
    {8193,12},{12289,12},
    {16385,13},{24577,13},
  }
  local function dist_sym_extra(dist)
    local lo, hi = 1, #DIST_TABLE
    while lo < hi do
      local mid = (lo + hi + 1) // 2
      if DIST_TABLE[mid][1] <= dist then lo = mid else hi = mid - 1 end
    end
    return lo - 1, DIST_TABLE[lo][2], DIST_TABLE[lo][1]
  end

  -- ── LZ77 match finder (hash chains over 3-byte sequences) ───────────────
  local MIN_MATCH, MAX_MATCH, WINDOW_SIZE, MAX_CHAIN = 3, 258, 32768, 128

  local function hash3(data, i)
    local b1, b2, b3 = data:byte(i, i + 2)
    return ((b1 * 33 + b2) * 33 + b3) % 32768
  end

  local function lz77_tokenize(data)
    local n = #data
    local tokens = {}
    local head, prev = {}, {}
    local i = 1

    while i <= n do
      local best_len, best_dist = 0, 0

      if i + MIN_MATCH - 1 <= n then
        local h = hash3(data, i)
        local cand = head[h]
        local chain = 0
        local window_lo = math.max(1, i - WINDOW_SIZE)

        while cand and cand >= window_lo and chain < MAX_CHAIN do
          if best_len == 0 or (cand + best_len <= n and
              data:byte(cand + best_len) == data:byte(i + best_len)) then
            local max_possible = math.min(MAX_MATCH, n - i + 1)
            local len = 0
            while len < max_possible and data:byte(cand + len) == data:byte(i + len) do
              len = len + 1
            end
            if len > best_len then
              best_len, best_dist = len, i - cand
              if best_len >= MAX_MATCH then break end
            end
          end
          cand = prev[cand]
          chain = chain + 1
        end
      end

      if best_len >= MIN_MATCH then
        tokens[#tokens + 1] = { match = true, len = best_len, dist = best_dist }
        local ins_end = math.min(i + best_len - 1, n - MIN_MATCH + 1)
        for k = i, ins_end do
          local h = hash3(data, k)
          prev[k] = head[h]
          head[h] = k
        end
        i = i + best_len
      else
        tokens[#tokens + 1] = { match = false, byte = data:byte(i) }
        if i + MIN_MATCH - 1 <= n then
          local h = hash3(data, i)
          prev[i] = head[h]
          head[h] = i
        end
        i = i + 1
      end
    end

    return tokens
  end

  local function encode_fixed_block(bw, tokens, is_final)
    bw_write_lsb(bw, is_final and 1 or 0, 1)  -- BFINAL
    bw_write_lsb(bw, 1, 2)                    -- BTYPE = 01 (fixed Huffman)

    for _, tok in ipairs(tokens) do
      if tok.match then
        local lsym, lextra, lbase = table.unpack(LEN_SYM_BASE[tok.len])
        local code, nbits = fixed_lit_code(lsym)
        bw_write_msb(bw, code, nbits)
        if lextra > 0 then bw_write_lsb(bw, tok.len - lbase, lextra) end

        local dsym, dextra, dbase = dist_sym_extra(tok.dist)
        local dcode, dnbits = fixed_dist_code(dsym)
        bw_write_msb(bw, dcode, dnbits)
        if dextra > 0 then bw_write_lsb(bw, tok.dist - dbase, dextra) end
      else
        local code, nbits = fixed_lit_code(tok.byte)
        bw_write_msb(bw, code, nbits)
      end
    end

    local code, nbits = fixed_lit_code(256)  -- end-of-block symbol
    bw_write_msb(bw, code, nbits)
  end

  --------------------------------------------------------------------------
  -- M.cpu_deflate_zlib(data) -> zlib-wrapped compressed bytes (string)
  --
  -- Self-contained: no external module required. Used as the energy-free
  -- fallback path for M.safe_deflate.
  --------------------------------------------------------------------------
  function M.cpu_deflate_zlib(data)
    local bw = new_bitwriter()
    bw.bytes[1] = 0x78  -- CMF: deflate, 32K window
    bw.bytes[2] = 0x9C  -- FLG: default level, FCHECK valid, FDICT=0

    if #data == 0 then
      encode_fixed_block(bw, {}, true)
    else
      encode_fixed_block(bw, lz77_tokenize(data), true)
    end

    local body  = bw_tostring(bw)
    local csum  = adler32(data)
    local trail = string.char(
      (csum >> 24) & 0xFF, (csum >> 16) & 0xFF, (csum >> 8) & 0xFF, csum & 0xFF
    )
    return body .. trail
  end
end

--------------------------------------------------------------------------------
-- Wrap the data component calls, falling back to CPU-based (pure-Lua)
-- inflate/deflate when the data component reports insufficient energy.
--
-- The data component's energy budget can be transient (a capacitor bank
-- recharging), so we still give it a couple of quick retries first; if it's
-- still starved after that we switch to the CPU path for this call rather
-- than blocking indefinitely on os.sleep(). If no CPU path is available at
-- all (cpu_deflate module missing, for inflate), we fall back to the old
-- unbounded retry-and-sleep behavior as a last resort.
--------------------------------------------------------------------------------
local ENERGY_RETRY_ATTEMPTS = 3

function M.safe_inflate(bytes)
  dbg("safe_inflate: attempting to inflate %d compressed bytes via data component", #bytes)
  local attempts = 0
  while true do
    attempts = attempts + 1
    local res, err = data_comp.inflate(bytes)

    if res then
      dbg("safe_inflate: success after %d attempt(s), inflated to %d bytes", attempts, #res)
      return res
    end

    if err and err:find("not enough energy", 1, true) then
      if M.cpu_deflate then
        if attempts < ENERGY_RETRY_ATTEMPTS then
          dbg("safe_inflate: attempt %d – not enough energy, sleeping 0.5s before retry", attempts)
          os.sleep(0.5)
        else
          dbg("safe_inflate: attempt %d – still not enough energy, falling back to CPU inflate", attempts)
          local ok, cpu_res = pcall(cpu_inflate_zlib_full, bytes)
          if ok then
            dbg("safe_inflate: CPU fallback success, inflated to %d bytes", #cpu_res)
            return cpu_res
          end
          dbg("safe_inflate: CPU fallback failed (%s), resuming energy retry", tostring(cpu_res))
          os.sleep(0.5)
        end
      else
        dbg("safe_inflate: attempt %d – not enough energy, sleeping 0.5s (no CPU fallback available)", attempts)
        os.sleep(0.5)
      end
    else
      dbg("safe_inflate: fatal inflate error after %d attempt(s): %s", attempts, tostring(err))
      error("inflate failed: " .. tostring(err))
    end
  end
end

function M.safe_deflate(bytes)
  dbg("safe_deflate: attempting to deflate %d bytes via data component", #bytes)
  local attempts = 0
  while true do
    attempts = attempts + 1
    local res, err = data_comp.deflate(bytes)

    if res then
      dbg("safe_deflate: success after %d attempt(s), deflated to %d bytes (%.1f%%)",
          attempts, #res, (#res / #bytes) * 100)
      return res
    end

    if err and (err:find("not enough energy", 1, true) or err:find("too little energy", 1, true)) then
      if attempts < ENERGY_RETRY_ATTEMPTS then
        dbg("safe_deflate: attempt %d – not enough energy, sleeping 0.5s before retry", attempts)
        os.sleep(0.5)
      else
        dbg("safe_deflate: attempt %d – still not enough energy, falling back to pure-Lua CPU deflate", attempts)
        local ok, cpu_res = pcall(M.cpu_deflate_zlib, bytes)
        if ok then
          dbg("safe_deflate: CPU fallback success, deflated to %d bytes (%.1f%%)",
              #cpu_res, (#cpu_res / #bytes) * 100)
          return cpu_res
        end
        dbg("safe_deflate: CPU fallback failed (%s), resuming energy retry", tostring(cpu_res))
        os.sleep(0.5)
      end
    else
      dbg("safe_deflate: fatal deflate error after %d attempt(s): %s", attempts, tostring(err))
      error("deflate failed: " .. tostring(err))
    end
  end
end

--------------------------------------------------------------------------------
-- make_inflater  (datacard path)
--
-- Returns a coroutine-style stepper function that binary-searches for the
-- exact compressed stream length, yielding control between each inflate call
-- so the data component's energy capacitor can recharge.
--
-- Returns: stepper() → result_or_nil, status
--   status == "retry"    → sleep and call again (energy wait)
--   status == "continue" → yield (os.sleep(0)) and call again
--   status == "done"     → result.data / result.next_pos are valid
--------------------------------------------------------------------------------
function M.make_inflater(data, pos, expected_size)
  local lo = 6
  -- Upper bound: worst-case zlib overhead is ~12 bytes; expected_size + 128 is
  -- a safe ceiling that keeps each inflate call within the component's energy
  -- budget (avoids the infinite retry loop that expected_size*2+128 could cause).
  local hi = math.min(
    math.max(expected_size + 128, 512),
    #data - pos + 1
  )

  if D.DEBUG_INFLATE then
    dbg("make_inflater: pos=%d expected_size=%d search_range=[%d,%d] data_remaining=%d",
        pos, expected_size, lo, hi, #data - pos + 1)
    dbg_hex("make_inflater: first bytes at pos", data:sub(pos, pos + 7), 8)
  end

  local best_result = nil
  local best_len    = nil
  local phase       = "verify"  -- then "search"
  local mid         = nil
  local step_count  = 0

  return function()
    step_count = step_count + 1

    -- ── Phase 1: verify upper bound once ──────────────────────────────────
    if phase == "verify" then
      if D.DEBUG_INFLATE then
        dbg("make_inflater[step %d]: VERIFY phase – trying hi=%d bytes", step_count, hi)
      end

      local res, err = component.data.inflate(data:sub(pos, pos + hi - 1))
      if not res then
        if err and err:find("not enough energy", 1, true) then
          if D.DEBUG_INFLATE then
            dbg("make_inflater[step %d]: VERIFY – not enough energy, requesting retry", step_count)
          end
          return nil, "retry"
        else
          dbg("make_inflater[step %d]: VERIFY – fatal inflate error: %s", step_count, tostring(err))
          error("inflate verify failed: " .. tostring(err))
        end
      end

      if D.DEBUG_INFLATE then
        dbg("make_inflater[step %d]: VERIFY OK – inflated %d bytes (expected %d), entering binary search",
            step_count, #res, expected_size)
      end

      assert(#res == expected_size,
        string.format("inflate_at: expected %d, got %d", expected_size, #res))

      best_result = res
      best_len    = hi
      phase       = "search"
      return nil, "continue"
    end

    -- ── Phase 2: binary search, one step per call ──────────────────────────
    if lo < hi then
      mid = math.floor((lo + hi) / 2)

      if D.DEBUG_INFLATE then
        dbg("make_inflater[step %d]: SEARCH lo=%d hi=%d mid=%d", step_count, lo, hi, mid)
      end

      local res, err = component.data.inflate(data:sub(pos, pos + mid - 1))
      if not res then
        if err and err:find("not enough energy", 1, true) then
          if D.DEBUG_INFLATE then
            dbg("make_inflater[step %d]: SEARCH mid=%d – not enough energy, requesting retry", step_count, mid)
          end
          return nil, "retry"
        end
        -- Invalid slice → too short; search higher
        if D.DEBUG_INFLATE then
          dbg("make_inflater[step %d]: SEARCH mid=%d – inflate failed (too short?), lo -> %d", step_count, mid, mid + 1)
        end
        lo = mid + 1
        return nil, "continue"
      end

      if #res == expected_size then
        if D.DEBUG_INFLATE then
          dbg("make_inflater[step %d]: SEARCH mid=%d – size match (%d bytes), narrowing hi -> %d",
              step_count, mid, #res, mid)
        end
        best_result = res
        best_len    = mid
        hi          = mid
      else
        if D.DEBUG_INFLATE then
          dbg("make_inflater[step %d]: SEARCH mid=%d – size mismatch (got %d, expected %d), lo -> %d",
              step_count, mid, #res, expected_size, mid + 1)
        end
        lo = mid + 1
      end

      return nil, "continue"
    end

    -- ── Done ──────────────────────────────────────────────────────────────
    -- OC's inflate converges to a best_len that overshoots the actual stream
    -- end by 4 bytes (the binary search can't narrow past the point where one
    -- byte less causes failure, but zlib silently ignores trailing bytes).
    -- Subtract 4 to land at the true end of the compressed stream.
    if D.DEBUG_INFLATE then
      dbg("make_inflater: DONE after %d steps – best_len=%d next_pos=%d (best_len - 4 overhead)",
          step_count, best_len, pos + best_len - 4)
    end
    return {
      data     = best_result,
      next_pos = pos + best_len - 4
    }, "done"
  end
end

--------------------------------------------------------------------------------
-- inflate_zlib_slice  (cpu path)
--
-- Unlike the datacard, libdeflate's inflate_zlib is a pure-Lua function that:
--   (a) has no energy budget, so no binary search is needed, and
--   (b) tracks exactly how many input bytes it consumed via its `bytes_read`
--       output field, giving us the true next_pos directly.
--
-- We pass the entire remaining packfile slice starting at `pos`; libdeflate
-- stops as soon as the zlib stream ends and reports how many bytes it read.
-- Returns: content, next_pos   (or asserts on failure)
--------------------------------------------------------------------------------
function M.inflate_zlib_slice(data, pos, expected_size)
  if not M.cpu_deflate then
    return nil, nil
  end

  -- ── Bounded input slice ───────────────────────────────────────────────────
  -- Deflate worst-case overhead is ~5 bytes per 32 KB block plus 6 bytes of
  -- zlib framing, so expected_size + 512 is safe for any object up to ~3 MB.
  -- We also keep a hard minimum of 1024 to handle tiny objects whose zlib
  -- framing is proportionally large.
  local available  = #data - pos + 1
  local input_hi   = math.min(math.max(expected_size + 512, 1024), available)
  local input_slice = data:sub(pos, pos + input_hi - 1)

  if D.DEBUG_INFLATE then
    dbg("inflate_zlib_slice: pos=%d expected_size=%d input_hi=%d available=%d (cpu)",
        pos, expected_size, input_hi, available)
    dbg_hex("inflate_zlib_slice: zlib header bytes", input_slice:sub(1, 6), 6)
  end

  -- ── Chunked output buffering ──────────────────────────────────────────────
  -- Accumulate into a fixed-size buffer rather than one string.char() per byte.
  local CHUNK_SIZE = 8192
  local chunks     = {}
  local buf        = {}
  local buf_n      = 0
  local total_len  = 0
  local bytes_read = 0

  local ok, err = pcall(function()
    silent_inflate_zlib({
      input  = input_slice,
      output = function(byte)
        buf_n      = buf_n + 1
        buf[buf_n] = string.char(byte)
        total_len  = total_len + 1
        if buf_n >= CHUNK_SIZE then
          chunks[#chunks + 1] = table.concat(buf, "", 1, buf_n)
          buf_n = 0
        end
      end,
      bytes_read_callback = function(n) bytes_read = n end,
    })
  end)

  input_slice = nil

  if not ok then
    dbg("inflate_zlib_slice: inflate_zlib error: %s", tostring(err))
    error("inflate_zlib_slice failed: " .. tostring(err))
  end

  if buf_n > 0 then
    chunks[#chunks + 1] = table.concat(buf, "", 1, buf_n)
  end
  buf = nil

  if D.DEBUG_INFLATE then
    dbg("inflate_zlib_slice: inflated %d bytes, bytes_read=%d next_pos=%d (expected_size=%d)",
        total_len, bytes_read, pos + bytes_read, expected_size)
  end

  assert(total_len == expected_size,
    string.format("inflate_zlib_slice: size mismatch: got %d, expected %d", total_len, expected_size))

  if bytes_read == 0 then
    dbg("inflate_zlib_slice: WARNING – bytes_read_callback not supported; falling back to bsearch")
    return M.inflate_zlib_slice_bsearch(data, pos, expected_size, nil)
  end

  if bytes_read >= input_hi then
    dbg("inflate_zlib_slice: WARNING – bytes_read (%d) reached input_hi (%d); " ..
        "stream may have been truncated, discarding content and falling back to bsearch",
        bytes_read, input_hi)
    return M.inflate_zlib_slice_bsearch(data, pos, expected_size, nil)
  end

  return chunks, pos + bytes_read
end

--------------------------------------------------------------------------------
-- inflate_zlib_slice_bsearch  (cpu fallback when bytes_read_callback absent)
--
-- Identical to the old inflate_zlib_slice: binary-searches for the shortest
-- input prefix that produces exactly expected_size bytes of output.
-- `known_chunks` is accepted for API compatibility but always discarded;
-- bsearch re-inflates cleanly from scratch.
--------------------------------------------------------------------------------
function M.inflate_zlib_slice_bsearch(data, pos, expected_size, known_chunks)
  local available   = #data - pos + 1
  local lo          = 6
  local hi          = math.min(math.max(expected_size + 512, 1024), available)
  local best_chunks = nil
  local best_len    = hi

  if D.DEBUG_INFLATE then
    dbg("inflate_zlib_slice_bsearch: pos=%d expected_size=%d search_range=[%d,%d] known_chunks=%s",
        pos, expected_size, lo, hi, known_chunks and "provided (discarded)" or "nil")
  end

  local CHUNK = 8192
  local function inflate_to_chunks(slice)
    local ch    = {}
    local buf   = {}
    local buf_n = 0
    local total = 0
    local ok = pcall(function()
      silent_inflate_zlib({
        input  = slice,
        output = function(byte)
          buf_n = buf_n + 1
          buf[buf_n] = string.char(byte)
          total = total + 1
          if buf_n >= CHUNK then
            ch[#ch + 1] = table.concat(buf, "", 1, buf_n)
            buf_n = 0
          end
        end,
      })
    end)
    if buf_n > 0 then ch[#ch + 1] = table.concat(buf, "", 1, buf_n) end
    if not ok then return nil, 0 end
    return ch, total
  end

  do
    local ch, total = inflate_to_chunks(data:sub(pos, pos + hi - 1))
    if ch and total == expected_size then
      best_chunks = ch
    else
      dbg("inflate_zlib_slice_bsearch: WARNING – hi=%d still too small, using full available=%d", hi, available)
      hi = available
      ch, total = inflate_to_chunks(data:sub(pos, pos + hi - 1))
      assert(ch and total == expected_size,
        string.format("inflate_zlib_slice_bsearch: even full slice failed: got %d, expected %d",
          total, expected_size))
      best_chunks = ch
    end
  end

  local step = 0
  while lo < hi do
    os.sleep(0)
    step = step + 1
    local mid = math.floor((lo + hi) / 2)
    local ch, total = inflate_to_chunks(data:sub(pos, pos + mid - 1))
    if ch and total == expected_size then
      if D.DEBUG_INFLATE then
        dbg("inflate_zlib_slice_bsearch[step %d]: mid=%d OK, narrowing hi", step, mid)
      end
      best_chunks = ch
      best_len    = mid
      hi          = mid
    else
      if D.DEBUG_INFLATE then
        dbg("inflate_zlib_slice_bsearch[step %d]: mid=%d failed, raising lo", step, mid)
      end
      lo = mid + 1
    end
  end

  if D.DEBUG_INFLATE then
    dbg("inflate_zlib_slice_bsearch: done after %d steps, best_len=%d next_pos=%d",
        step, best_len, pos + best_len)
  end

  assert(best_chunks, "inflate_zlib_slice_bsearch: failed to find stream boundary")
  return best_chunks, pos + best_len
end

--------------------------------------------------------------------------------
-- chunks_to_string
-- Materialise a chunks table into one Lua string.  Call only when a single
-- contiguous string is genuinely required (hashing, concatenation, etc.).
--------------------------------------------------------------------------------
function M.chunks_to_string(chunks)
  return table.concat(chunks)
end

--------------------------------------------------------------------------------
-- inflate_pack_object
--
-- Public entry point.  Returns: chunks (table of strings), next_pos (integer).
-- Call M.chunks_to_string(chunks) if a single string is needed.
--------------------------------------------------------------------------------
function M.inflate_pack_object(data, pos, expected_size)
  if D.DEBUG_INFLATE then
    dbg("inflate_pack_object: pos=%d expected_size=%d path=%s",
        pos, expected_size, M.cpu_deflate and "cpu" or "datacard")
  end

  if M.cpu_deflate then
    if D.DEBUG_INFLATE then
      dbg("inflate_pack_object: using cpu_deflate path (inflate_zlib_slice)")
    end
    local chunks, next_pos = M.inflate_zlib_slice(data, pos, expected_size)
    assert(chunks, "inflate failed: cpu inflater returned no output")
    if D.DEBUG_INFLATE then
      dbg("inflate_pack_object: cpu path done, next_pos=%d consumed=%d bytes",
          next_pos, next_pos - pos)
    end
    return chunks, next_pos
  end

  if D.DEBUG_INFLATE then
    dbg("inflate_pack_object: using datacard path (make_inflater binary search)")
  end
  local inflater   = M.make_inflater(data, pos, expected_size)
  local loop_count = 0
  while true do
    loop_count = loop_count + 1
    local out, status = inflater()

    if status == "retry" then
      if D.DEBUG_INFLATE then
        dbg("inflate_pack_object: loop %d – retry (energy wait 0.5s)", loop_count)
      end
      os.sleep(0.5)
    elseif status == "continue" then
      os.sleep(0)
    elseif status == "done" then
      if D.DEBUG_INFLATE then
        dbg("inflate_pack_object: datacard path done after %d loops, next_pos=%d consumed=%d bytes",
            loop_count, out.next_pos, out.next_pos - pos)
      end
      -- Wrap datacard string in a single-element table for a uniform API.
      return {out.data}, out.next_pos
    end
  end
end

return M