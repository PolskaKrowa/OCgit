-- protocol.lua
-- Git smart-HTTP protocol v2 client:
--   discover_refs      – ls-refs (with v1 fallback)
--   negotiate_packfile – fetch command
--   demux_sideband     – split the multiplexed server response into pack data

local util = require("util")
local http_request    = util.http_request
local pkt_line        = util.pkt_line
local parse_pkt_lines = util.parse_pkt_lines

local M = {}

--------------------------------------------------------------------------------
-- Step 1: Discover available refs (ls-refs)
--
-- Protocol v2 works in two stages:
--   GET  /info/refs?service=git-upload-pack   → capability advertisement only
--   POST /git-upload-pack  with ls-refs cmd   → actual ref listing
--------------------------------------------------------------------------------
function M.discover_refs(remote_url)
  local function dump_raw(label, raw)
    print("[DEBUG] " .. label .. " (" .. #raw .. " bytes):")
    -- Print first 512 bytes as escaped text so we can see the pkt-line framing
    local preview = raw:sub(1, 512):gsub("[%c]", function(c)
      local b = c:byte()
      if b == 10 then return "\\n"
      elseif b == 13 then return "\\r"
      elseif b == 0  then return "\\0"
      else return string.format("\\x%02x", b) end
    end)
    print(preview)
    print("[DEBUG] parsed pkt-lines:")
    for i, l in ipairs(parse_pkt_lines(raw)) do
      print(string.format("  [%d] %s", i, tostring(l):sub(1, 120):gsub("[%c]",".")))
      if i >= 20 then print("  ... (truncated)"); break end
    end
  end

  -- Stage 1: capability advertisement
  local info_url   = remote_url .. "/info/refs?service=git-upload-pack"
  local v2_headers = { ["Git-Protocol"] = "version=2" }

  print("Discovering refs via Protocol v2...")
  local info_resp = http_request(info_url, nil, v2_headers)
  dump_raw("info/refs response", info_resp)

  if not info_resp:find("version 2", 1, true) then
    print("[DEBUG] server did not advertise v2 – trying v1 fallback")
    local lines = parse_pkt_lines(info_resp)
    local refs  = {}
    for _, line in ipairs(lines) do
      -- v1: "<sha> <ref>\0<capabilities>" on first line, "<sha> <ref>" after
      local sha, ref = line:match("^([0-9a-f]+)%s+(refs/heads/[^%z%s]+)")
      if sha and ref then refs[ref] = sha end
    end
    if next(refs) then return refs end
    error("Server does not support Git protocol v2 and no v1 refs found")
  end

  -- Stage 2: ls-refs command
  local upload_url = remote_url .. "/git-upload-pack"
  local ls_headers = {
    ["Git-Protocol"]  = "version=2",
    ["Content-Type"]  = "application/x-git-upload-pack-request",
    ["Accept"]        = "application/x-git-upload-pack-result",
  }

  local ls_payload = table.concat({
    pkt_line("command=ls-refs\n"),
    pkt_line("DELIM"),
    pkt_line("symrefs\n"),
    pkt_line("peel\n"),
    pkt_line("FLUSH"),
  })

  print("[DEBUG] ls-refs payload: " .. ls_payload:gsub("[%c]", "."))

  local ls_resp = http_request(upload_url, ls_payload, ls_headers)
  dump_raw("ls-refs response", ls_resp)

  local lines = parse_pkt_lines(ls_resp)
  local refs  = {}
  for _, line in ipairs(lines) do
    local sha, ref = line:match("^([0-9a-f]+)%s+(refs/heads/%S+)")
    if sha and ref then refs[ref] = sha end
  end

  return refs
end

--------------------------------------------------------------------------------
-- Step 2: Request the Packfile (fetch)
--------------------------------------------------------------------------------
function M.negotiate_packfile(remote_url, want_sha)
  local url = remote_url .. "/git-upload-pack"
  local headers = {
    ["Git-Protocol"]  = "version=2",
    ["Content-Type"]  = "application/x-git-upload-pack-request",
    ["Accept"]        = "application/x-git-upload-pack-result",
  }

  local payload = table.concat({
    pkt_line("command=fetch\n"),
    pkt_line("DELIM"),
    pkt_line("thin-pack\n"),
    pkt_line("ofs-delta\n"),
    pkt_line("want " .. want_sha .. "\n"),
    pkt_line("done\n"),
    pkt_line("FLUSH"),
  })

  print("Requesting packfile for " .. want_sha .. "...")
  return http_request(url, payload, headers)
end

--------------------------------------------------------------------------------
-- Step 3: Demultiplex the Sideband
--
-- Every pkt-line payload begins with a channel byte:
--   \1  Packfile binary data  → collect into a buffer
--   \2  Progress message      → print to console
--   \3  Fatal error           → raise an error
--------------------------------------------------------------------------------
function M.demux_sideband(response)
  local lines  = parse_pkt_lines(response)
  local chunks = {}

  for _, line in ipairs(lines) do
    if line ~= "FLUSH" and line ~= "DELIM" and line ~= "RESPONSE_END" then
      local channel = line:byte(1)

      -- Skip section headers ("packfile\n", "acknowledgments\n", etc.)
      -- Real sideband lines always start with 0x01, 0x02, or 0x03
      if channel == 1 or channel == 2 or channel == 3 then
        local payload = line:sub(2)
        if channel == 1 then
          chunks[#chunks + 1] = payload
        elseif channel == 2 then
          io.write(payload)
        elseif channel == 3 then
          error("Remote error: " .. payload)
        end
      end
    end
  end

  return table.concat(chunks)
end

return M
