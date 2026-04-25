-- inflate.lua
-- Zlib inflate/deflate wrappers and the binary-search stream-boundary finder.
--
-- Prefers a pure-Lua cpu_deflate implementation (libdeflate / deflate.lua) and
-- falls back to the OpenComputers data component when it is unavailable.

local component = require("component")
local data_comp = component.data

local filesystem = require("filesystem")
 
local D       = require("debug")
local dbg       = D.dbg
local dbg_hex   = D.dbg_hex
local dbg_enter = D.dbg_enter
local dbg_leave = D.dbg_leave
 
local util    = require("util")
local sha1         = util.sha1
local read_u32_be  = util.read_u32_be
 
local inflate = require("pack_inflate")
local safe_deflate        = inflate.safe_deflate
local inflate_pack_object = inflate.inflate_pack_object
local chunks_to_string    = inflate.chunks_to_string

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

  if D.DEBUG_INFLATE and #captured > 0 then
    for _, line in ipairs(captured) do
      dbg("inflate_zlib (suppressed): %s", line)
    end
  end

  if not ok then error(err, 2) end
end
-- Wrap the data component calls, retrying on "not enough energy" errors.
--------------------------------------------------------------------------------
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
      dbg("safe_inflate: attempt %d – not enough energy, sleeping 0.5s", attempts)
      os.sleep(0.5)
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
      dbg("safe_deflate: attempt %d – not enough energy, sleeping 0.5s", attempts)
      os.sleep(0.5)
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

function M.write_object(git_dir, sha, type_name, content)
  dbg("write_object: deferred (RAM-only) %s %s", type_name, sha)
end

--------------------------------------------------------------------------------
-- Parse the Packfile
--
-- Header layout:
--   Bytes 1-4  : Magic "PACK"
--   Bytes 5-8  : Version (big-endian u32, usually 2)
--   Bytes 9-12 : Object count (big-endian u32)
--
-- Per-object layout:
--   Variable-length type+size header (MSB = more bytes follow)
--   For OFS_DELTA : variable-length negative offset (MSB-encoded)
--   For REF_DELTA : 20 raw bytes (binary SHA-1 of base object)
--   Followed by zlib-deflated object data
--------------------------------------------------------------------------------
local TYPE_NAMES = { [1]="commit", [2]="tree", [3]="blob", [4]="tag" }
 
function M.parse_packfile(pack_data, git_dir)
  dbg_enter("parse_packfile")
 
  -- ── Header ──────────────────────────────────────────────────────────────
  assert(pack_data:sub(1, 4) == "PACK", "Invalid packfile magic")
  local version     = read_u32_be(pack_data, 5)
  local num_objects = read_u32_be(pack_data, 9)
  print(string.format("Packfile v%d – %d objects", version, num_objects))
  dbg("parse_packfile: total packfile size=%d bytes, objects start at pos=13", #pack_data)
 
  local pos = 13  -- byte position in pack_data (1-indexed)
 
  local objects    = {}  -- sha  -> { type=string, data=string }
  local off_to_sha = {}  -- file_offset -> sha  (for OFS_DELTA resolution)
  local ofs_queue  = {}  -- pending OFS_DELTA entries
  local ref_queue  = {}  -- pending REF_DELTA entries
 
  local type_counts = { commit=0, tree=0, blob=0, tag=0, ofs_delta=0, ref_delta=0, unknown=0 }
 
  -- ── Object loop ──────────────────────────────────────────────────────────
  for obj_idx = 1, num_objects do
    local obj_start = pos
 
    -- Read the variable-length type+size header
    local byte     = pack_data:byte(pos); pos = pos + 1
    local obj_type = (byte >> 4) & 0x7
    local size     = byte & 0xF
    local shift    = 4
 
    while byte & 0x80 ~= 0 do
      byte  = pack_data:byte(pos); pos = pos + 1
      size  = size | ((byte & 0x7F) << shift)
      shift = shift + 7
    end
 
    local type_label = TYPE_NAMES[obj_type]
        or (obj_type == 6 and "OFS_DELTA")
        or (obj_type == 7 and "REF_DELTA")
        or string.format("UNKNOWN(%d)", obj_type)
 
    dbg("parse_packfile: obj#%d/%d offset=%d type=%s(%d) declared_size=%d",
        obj_idx, num_objects, obj_start, type_label, obj_type, size)
 
    if obj_type == 6 then
      -- ── OFS_DELTA: base is at (obj_start - offset) ────────────────────
      -- The offset is encoded with a "bijective base-128" scheme
      byte = pack_data:byte(pos); pos = pos + 1
      local offset = byte & 0x7F
      while byte & 0x80 ~= 0 do
        byte   = pack_data:byte(pos); pos = pos + 1
        offset = ((offset + 1) << 7) | (byte & 0x7F)
      end
 
      local base_offset = obj_start - offset
      dbg("parse_packfile: OFS_DELTA encoded_offset=%d base_file_offset=%d", offset, base_offset)
 
      local inflate_start_pos = pos
      local delta_data, next_pos = inflate_pack_object(pack_data, pos, size)
      delta_data = chunks_to_string(delta_data)
      pos = next_pos
 
      dbg("parse_packfile: OFS_DELTA inflated delta=%d bytes, compressed_span=%d bytes",
          #delta_data, next_pos - inflate_start_pos)
 
      ofs_queue[#ofs_queue + 1] = {
        obj_offset  = obj_start,
        base_offset = base_offset,
        delta       = delta_data,
      }
      type_counts.ofs_delta = type_counts.ofs_delta + 1
 
    elseif obj_type == 7 then
      -- ── REF_DELTA: base identified by its 20-byte binary SHA ──────────
      local base_sha = ""
      for j = 0, 19 do
        base_sha = base_sha .. string.format("%02x", pack_data:byte(pos + j))
      end
      pos = pos + 20
 
      dbg("parse_packfile: REF_DELTA base_sha=%s", base_sha)
 
      local inflate_start_pos = pos
      local delta_data, next_pos = inflate_pack_object(pack_data, pos, size)
      delta_data = chunks_to_string(delta_data)
      pos = next_pos
 
      dbg("parse_packfile: REF_DELTA inflated delta=%d bytes, compressed_span=%d bytes",
          #delta_data, next_pos - inflate_start_pos)
 
      ref_queue[#ref_queue + 1] = {
        obj_offset = obj_start,
        base_sha   = base_sha,
        delta      = delta_data,
      }
      type_counts.ref_delta = type_counts.ref_delta + 1
 
    else
      -- ── Regular object (commit / tree / blob / tag) ───────────────────
      local type_name = TYPE_NAMES[obj_type]
      if not type_name then
        dbg("parse_packfile: WARNING – unknown obj_type=%d at offset=%d, skipping", obj_type, obj_start)
        type_counts.unknown = type_counts.unknown + 1
      else
        local inflate_start_pos = pos
        local content, next_pos = inflate_pack_object(pack_data, pos, size)
        content = chunks_to_string(content)
        pos = next_pos
 
        dbg("parse_packfile: %s inflated content=%d bytes compressed_span=%d bytes",
            type_name, #content, next_pos - inflate_start_pos)
 
        local store = type_name .. " " .. #content .. "\0" .. content
        local sha   = sha1(store)
 
        objects[sha]          = { type = type_name, data = content }
        off_to_sha[obj_start] = sha
 
        if type_name == "blob" then
          -- Extra blob diagnostics: show a printable snippet of content
          local snippet = content:sub(1, 64):gsub("[%c]", ".")
          dbg("parse_packfile: blob snippet: %s", snippet)
        end
 
        M.write_object(git_dir, sha, type_name, content)
        print(string.format("  [%s] %s", type_name, sha))
        type_counts[type_name] = (type_counts[type_name] or 0) + 1
      end
    end
  end
 
  dbg("parse_packfile: first pass complete – regular objects stored=%d, ofs_queue=%d, ref_queue=%d",
      (function() local n=0 for _ in pairs(objects) do n=n+1 end return n end)(),
      #ofs_queue, #ref_queue)
  dbg("parse_packfile: type breakdown – commit=%d tree=%d blob=%d tag=%d ofs_delta=%d ref_delta=%d unknown=%d",
      type_counts.commit, type_counts.tree, type_counts.blob, type_counts.tag,
      type_counts.ofs_delta, type_counts.ref_delta, type_counts.unknown)
 
  -- ── Delta resolution (multiple passes for chained deltas) ──────────────
  local pass_num = 0
  -- How many apply_delta calls to run before a longer energy-recharge sleep.
  -- Each call is far heavier than a single inflate command, so the threshold
  -- is lower than apply_delta's own YIELD_EVERY (32 commands).
  local RESOLVE_YIELD_EVERY = 8
  local total_applied = 0   -- shared across all passes and both queues
 
  local function resolve_pass()
    pass_num = pass_num + 1
    local resolved = 0
    os.sleep(0)
 
    dbg("resolve_pass #%d: ofs_queue=%d ref_queue=%d off_to_sha_entries=%d objects=%d",
        pass_num, #ofs_queue, #ref_queue,
        (function() local n=0 for _ in pairs(off_to_sha) do n=n+1 end return n end)(),
        (function() local n=0 for _ in pairs(objects)    do n=n+1 end return n end)())
 
    -- OFS_DELTA
    local remaining_ofs = {}
    for _, entry in ipairs(ofs_queue) do
      local base_sha = off_to_sha[entry.base_offset]
      local base_obj = base_sha and objects[base_sha]
 
      if not base_sha then
        dbg("resolve_pass #%d: OFS_DELTA @ offset=%d – base_offset=%d NOT in off_to_sha (deferred)",
            pass_num, entry.obj_offset, entry.base_offset)
      elseif not base_obj then
        dbg("resolve_pass #%d: OFS_DELTA @ offset=%d – base_sha=%s found but object NOT in objects table (deferred)",
            pass_num, entry.obj_offset, base_sha)
      end
 
      if base_obj then
        dbg("resolve_pass #%d: OFS_DELTA @ offset=%d – applying delta (base=%s type=%s base_size=%d delta_size=%d)",
            pass_num, entry.obj_offset, base_sha, base_obj.type, #base_obj.data, #entry.delta)
 
        local ok, result = pcall(M.apply_delta, base_obj.data, entry.delta)
        if not ok then
          dbg("resolve_pass #%d: OFS_DELTA @ offset=%d – apply_delta ERROR: %s",
              pass_num, entry.obj_offset, tostring(result))
          error(result)
        end
        local content   = result
        local type_name = base_obj.type
        local store     = type_name .. " " .. #content .. "\0" .. content
        local sha       = sha1(store)
 
        dbg("resolve_pass #%d: OFS_DELTA resolved → sha=%s type=%s content_size=%d",
            pass_num, sha, type_name, #content)
 
        objects[sha]                 = { type = type_name, data = content }
        off_to_sha[entry.obj_offset] = sha
 
        M.write_object(git_dir, sha, type_name, content)
        print(string.format("  [%s/ofs_delta] %s", type_name, sha))
        resolved      = resolved + 1
        total_applied = total_applied + 1
        if total_applied % RESOLVE_YIELD_EVERY == 0 then
          dbg("resolve_pass #%d: energy yield after %d total apply_delta calls", pass_num, total_applied)
          os.sleep(0.5)
        else
          os.sleep(0)
        end
      else
        remaining_ofs[#remaining_ofs + 1] = entry
      end
    end
    ofs_queue = remaining_ofs
 
    -- REF_DELTA
    local remaining_ref = {}
    for _, entry in ipairs(ref_queue) do
      local base_obj = objects[entry.base_sha]
 
      if not base_obj then
        dbg("resolve_pass #%d: REF_DELTA @ offset=%d – base_sha=%s NOT in objects table (deferred)",
            pass_num, entry.obj_offset, entry.base_sha)
      end
 
      if base_obj then
        dbg("resolve_pass #%d: REF_DELTA @ offset=%d – applying delta (base=%s type=%s base_size=%d delta_size=%d)",
            pass_num, entry.obj_offset, entry.base_sha, base_obj.type, #base_obj.data, #entry.delta)
 
        local ok, result = pcall(M.apply_delta, base_obj.data, entry.delta)
        if not ok then
          dbg("resolve_pass #%d: REF_DELTA @ offset=%d – apply_delta ERROR: %s",
              pass_num, entry.obj_offset, tostring(result))
          error(result)
        end
        local content   = result
        local type_name = base_obj.type
        local store     = type_name .. " " .. #content .. "\0" .. content
        local sha       = sha1(store)
 
        dbg("resolve_pass #%d: REF_DELTA resolved → sha=%s type=%s content_size=%d",
            pass_num, sha, type_name, #content)
 
        objects[sha]                 = { type = type_name, data = content }
        off_to_sha[entry.obj_offset] = sha
 
        M.write_object(git_dir, sha, type_name, content)
        print(string.format("  [%s/ref_delta] %s", type_name, sha))
        resolved      = resolved + 1
        total_applied = total_applied + 1
        if total_applied % RESOLVE_YIELD_EVERY == 0 then
          dbg("resolve_pass #%d: energy yield after %d total apply_delta calls", pass_num, total_applied)
          os.sleep(0.5)
        else
          os.sleep(0)
        end
      else
        remaining_ref[#remaining_ref + 1] = entry
      end
    end
    ref_queue = remaining_ref
 
    dbg("resolve_pass #%d: resolved=%d this pass, ofs_remaining=%d ref_remaining=%d",
        pass_num, resolved, #ofs_queue, #ref_queue)
    return resolved
  end
 
  -- Keep resolving until nothing left (handles chained deltas)
  dbg("parse_packfile: starting delta resolution loop (ofs=%d ref=%d)", #ofs_queue, #ref_queue)
  repeat until resolve_pass() == 0
  dbg("parse_packfile: delta resolution complete after %d pass(es)", pass_num)
 
  if #ofs_queue + #ref_queue > 0 then
    dbg("parse_packfile: WARNING – %d unresolved OFS + %d unresolved REF deltas remain",
        #ofs_queue, #ref_queue)
    for i, e in ipairs(ofs_queue) do
      dbg("  unresolved OFS #%d: obj_offset=%d base_offset=%d (base_sha=%s)",
          i, e.obj_offset, e.base_offset, tostring(off_to_sha[e.base_offset]))
    end
    for i, e in ipairs(ref_queue) do
      dbg("  unresolved REF #%d: obj_offset=%d base_sha=%s (in_objects=%s)",
          i, e.obj_offset, e.base_sha, tostring(objects[e.base_sha] ~= nil))
    end
    print(string.format("WARNING: %d delta(s) could not be resolved (missing base objects)",
      #ofs_queue + #ref_queue))
  end
 
  dbg("parse_packfile: final object count=%d",
      (function() local n=0 for _ in pairs(objects) do n=n+1 end return n end)())
  dbg_leave("parse_packfile")
  return objects
end

return M