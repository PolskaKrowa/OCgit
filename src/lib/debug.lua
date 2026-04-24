-- debug.lua
-- Debug flags and logging helpers.
--
-- Set DEBUG = true to enable verbose tracing throughout the script.
-- Set DEBUG_INFLATE = true for extra per-slice inflate tracing (very noisy).
-- Set DEBUG_DELTA   = true for per-instruction delta tracing (extremely noisy).

local M = {}

M.DEBUG         = true   -- master switch
M.DEBUG_INFLATE = true   -- inflate / zlib-slice tracing
M.DEBUG_DELTA   = true   -- apply_delta instruction tracing

local _indent = 0

function M.dbg(fmt, ...)
  if not M.DEBUG then return end
  local prefix = string.rep("  ", _indent) .. "[DBG] "
  print(prefix .. string.format(fmt, ...))
end

function M.dbg_enter(label)
  if not M.DEBUG then return end
  print(string.rep("  ", _indent) .. "[DBG] --> " .. label)
  _indent = _indent + 1
end

function M.dbg_leave(label)
  if not M.DEBUG then return end
  _indent = math.max(0, _indent - 1)
  print(string.rep("  ", _indent) .. "[DBG] <-- " .. label)
end

function M.dbg_hex(label, s, max_bytes)
  if not M.DEBUG then return end
  max_bytes = max_bytes or 32
  local out = {}
  for i = 1, math.min(#s, max_bytes) do
    out[#out + 1] = string.format("%02x", s:byte(i))
  end
  local suffix = (#s > max_bytes) and string.format(" ... (%d total bytes)", #s) or ""
  M.dbg("%s: %s%s", label, table.concat(out, " "), suffix)
end

return M
