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
-- safe_inflate / safe_deflate
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

  -- ── bound the input slice ──────────────────────────────────────────
  -- The compressed stream can't exceed expected_size + small zlib overhead,
  -- so cap the input the same way make_inflater / bsearch already do.
  local input_hi    = math.min(math.max(expected_size + 128, 512), #data - pos + 1)
  local input_slice = data:sub(pos, pos + input_hi - 1)

  if D.DEBUG_INFLATE then
    dbg("inflate_zlib_slice: pos=%d expected_size=%d input_slice_len=%d (cpu)",
        pos, expected_size, #input_slice)
    dbg_hex("inflate_zlib_slice: zlib header bytes", input_slice:sub(1, 6), 6)
  end

  -- ── chunked output buffering ───────────────────────────────────────
  -- Accumulate into a small fixed-size buffer and flush it as a single
  -- concatenated chunk, keeping peak live-object count down to CHUNK_SIZE + a
  -- handful of chunk strings.
  local CHUNK_SIZE = 512        -- tune down further if RAM is extremely tight
  local chunks     = {}
  local buf        = {}
  local buf_n      = 0
  local bytes_read = 0

  local ok, err = pcall(function()
    M.cpu_deflate.inflate_zlib({
      input  = input_slice,
      output = function(byte)
        buf_n       = buf_n + 1
        buf[buf_n]  = string.char(byte)
        if buf_n >= CHUNK_SIZE then
          chunks[#chunks + 1] = table.concat(buf, "", 1, buf_n)
          buf_n = 0
        end
      end,
      bytes_read_callback = function(n) bytes_read = n end,
    })
  end)

  -- Release the (now large) input slice as early as possible
  input_slice = nil

  if not ok then
    dbg("inflate_zlib_slice: inflate_zlib error: %s", tostring(err))
    error("inflate_zlib_slice failed: " .. tostring(err))
  end

  -- Flush any remaining bytes in the partial buffer
  if buf_n > 0 then
    chunks[#chunks + 1] = table.concat(buf, "", 1, buf_n)
  end
  buf = nil  -- release per-byte buffer before the final concat

  local content = table.concat(chunks)
  chunks = nil   -- release chunk list immediately after

  if D.DEBUG_INFLATE then
    dbg("inflate_zlib_slice: inflated %d bytes, bytes_read=%d next_pos=%d (expected_size=%d)",
        #content, bytes_read, pos + bytes_read, expected_size)
  end

  assert(#content == expected_size,
    string.format("inflate_zlib_slice: size mismatch: got %d, expected %d", #content, expected_size))

  if bytes_read == 0 then
    dbg("inflate_zlib_slice: WARNING – bytes_read_callback not supported by this libdeflate build; " ..
        "falling back to binary search to find stream boundary")
    return M.inflate_zlib_slice_bsearch(data, pos, expected_size, content)
  end

  return content, pos + bytes_read
end

--------------------------------------------------------------------------------
-- inflate_zlib_slice_bsearch  (cpu fallback when bytes_read_callback absent)
--
-- Identical to the old inflate_zlib_slice: binary-searches for the shortest
-- input prefix that produces exactly expected_size bytes of output.
-- `known_content` is the already-inflated result so we skip re-inflating hi.
--------------------------------------------------------------------------------
function M.inflate_zlib_slice_bsearch(data, pos, expected_size, known_content)
  local lo = 6
  local hi = math.min(math.max(expected_size + 128, 512), #data - pos + 1)
  local best_content = known_content
  local best_len     = hi  -- we already know hi works (inflate_zlib_slice verified it)

  if D.DEBUG_INFLATE then
    dbg("inflate_zlib_slice_bsearch: pos=%d expected_size=%d search_range=[%d,%d]",
        pos, expected_size, lo, hi)
  end

  local step = 0
  while lo < hi do
    os.sleep(0)
    step = step + 1
    local mid = math.floor((lo + hi) / 2)
    local out  = {}
    local ok   = pcall(function()
      M.cpu_deflate.inflate_zlib({
        input  = data:sub(pos, pos + mid - 1),
        output = function(byte) out[#out + 1] = string.char(byte) end,
      })
    end)
    local content = ok and table.concat(out) or nil
    if content and #content == expected_size then
      if D.DEBUG_INFLATE then
        dbg("inflate_zlib_slice_bsearch[step %d]: mid=%d OK, narrowing hi", step, mid)
      end
      best_content = content
      best_len     = mid
      hi           = mid
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

  assert(best_content, "inflate_zlib_slice_bsearch: failed to find stream boundary")
  return best_content, pos + best_len
end

--------------------------------------------------------------------------------
-- inflate_pack_object
--
-- Public entry point: inflates one pack object starting at `pos` in `data`,
-- dispatching to either the cpu or datacard path.
-- Returns: content (string), next_pos (integer)
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
    local content, next_pos = M.inflate_zlib_slice(data, pos, expected_size)
    assert(content, "inflate failed: cpu inflater returned no output")
    assert(#content == expected_size,
      string.format("inflate_at: expected %d, got %d", expected_size, #content))
    if D.DEBUG_INFLATE then
      dbg("inflate_pack_object: cpu path done, next_pos=%d consumed=%d bytes",
          next_pos, next_pos - pos)
    end
    return content, next_pos
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
      return out.data, out.next_pos
    end
  end
end

return M