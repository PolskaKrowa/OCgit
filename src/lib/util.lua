-- util.lua
-- Pure-Lua utility functions: SHA-1, big-endian u32 read,
-- pkt-line formatting/parsing, and raw HTTP requests.

local internet = require("internet")

local M = {}

--------------------------------------------------------------------------------
-- Pure-Lua SHA-1
--------------------------------------------------------------------------------
function M.sha1(msg)
  local function rotate(x, n)
    return ((x << n) | (x >> (32 - n))) & 0xFFFFFFFF
  end

  local h0 = 0x67452301
  local h1 = 0xEFCDAB89
  local h2 = 0x98BADCFE
  local h3 = 0x10325476
  local h4 = 0xC3D2E1F0

  -- Padding
  local msg_len = #msg
  msg = msg .. "\128"
  while #msg % 64 ~= 56 do msg = msg .. "\0" end

  -- Append original bit-length as 64-bit big-endian
  local bit_len = msg_len * 8
  msg = msg .. string.char(
    (bit_len >> 56) & 0xFF, (bit_len >> 48) & 0xFF,
    (bit_len >> 40) & 0xFF, (bit_len >> 32) & 0xFF,
    (bit_len >> 24) & 0xFF, (bit_len >> 16) & 0xFF,
    (bit_len >>  8) & 0xFF,  bit_len        & 0xFF
  )

  for i = 1, #msg, 64 do
    local chunk = msg:sub(i, i + 63)
    local w = {}
    for j = 0, 15 do
      local b1, b2, b3, b4 = chunk:byte(j*4+1, j*4+4)
      w[j] = ((b1 << 24) | (b2 << 16) | (b3 << 8) | b4) & 0xFFFFFFFF
    end
    for j = 16, 79 do
      w[j] = rotate(w[j-3] ~ w[j-8] ~ w[j-14] ~ w[j-16], 1)
    end

    local a, b, c, d, e = h0, h1, h2, h3, h4
    for j = 0, 79 do
      local f, k
      if j < 20 then
        f = ((b & c) | ((~b) & d)) & 0xFFFFFFFF; k = 0x5A827999
      elseif j < 40 then
        f = (b ~ c ~ d)            & 0xFFFFFFFF; k = 0x6ED9EBA1
      elseif j < 60 then
        f = ((b & c) | (b & d) | (c & d)) & 0xFFFFFFFF; k = 0x8F1BBCDC
      else
        f = (b ~ c ~ d)            & 0xFFFFFFFF; k = 0xCA62C1D6
      end
      local temp = (rotate(a, 5) + f + e + k + w[j]) & 0xFFFFFFFF
      e = d; d = c; c = rotate(b, 30); b = a; a = temp
    end

    h0 = (h0 + a) & 0xFFFFFFFF; h1 = (h1 + b) & 0xFFFFFFFF
    h2 = (h2 + c) & 0xFFFFFFFF; h3 = (h3 + d) & 0xFFFFFFFF
    h4 = (h4 + e) & 0xFFFFFFFF
  end

  return string.format("%08x%08x%08x%08x%08x", h0, h1, h2, h3, h4)
end

--------------------------------------------------------------------------------
-- Read a big-endian u32 from a string at position pos
--------------------------------------------------------------------------------
function M.read_u32_be(data, pos)
  local b1, b2, b3, b4 = data:byte(pos, pos + 3)
  return ((b1 << 24) | (b2 << 16) | (b3 << 8) | b4) & 0xFFFFFFFF
end

--------------------------------------------------------------------------------
-- Format a string into a pkt-line
--------------------------------------------------------------------------------
function M.pkt_line(text)
  if text == "FLUSH"        then return "0000" end
  if text == "DELIM"        then return "0001" end
  if text == "RESPONSE_END" then return "0002" end
  local len = #text + 4
  return string.format("%04x%s", len, text)
end

--------------------------------------------------------------------------------
-- Parse a stream of pkt-lines into a table of strings
--------------------------------------------------------------------------------
function M.parse_pkt_lines(raw_data)
  local lines = {}
  local pos = 1
  while pos <= #raw_data do
    local hex_len = raw_data:sub(pos, pos + 3)
    if #hex_len < 4 then break end
    local len = tonumber(hex_len, 16)
    if not len then break end

    if len == 0 then
      lines[#lines + 1] = "FLUSH";        pos = pos + 4
    elseif len == 1 then
      lines[#lines + 1] = "DELIM";        pos = pos + 4
    elseif len == 2 then
      lines[#lines + 1] = "RESPONSE_END"; pos = pos + 4
    else
      lines[#lines + 1] = raw_data:sub(pos + 4, pos + len - 1)
      pos = pos + len
    end
  end
  return lines
end

--------------------------------------------------------------------------------
-- Raw HTTP request; returns the full response body as a string
--------------------------------------------------------------------------------
function M.http_request(url, post_data, headers)
  local req = internet.request(url, post_data, headers)
  local chunks = {}
  for chunk in req do chunks[#chunks + 1] = chunk end
  return table.concat(chunks)
end

return M
