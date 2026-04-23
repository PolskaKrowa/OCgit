-- gitlite.lua - A minimal git clone implementation for OpenOS

local component = require("component")
local filesystem = require("filesystem")
local internet   = require("internet")
local data_comp  = component.data

--------------------------------------------------------------------------------
-- Pure-Lua SHA-1
--------------------------------------------------------------------------------
local function sha1(msg)
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
-- Utility: read a big-endian u32 from a string at position pos
--------------------------------------------------------------------------------
local function read_u32_be(data, pos)
  local b1, b2, b3, b4 = data:byte(pos, pos + 3)
  return ((b1 << 24) | (b2 << 16) | (b3 << 8) | b4) & 0xFFFFFFFF
end

--------------------------------------------------------------------------------
-- Utility to format a string into a pkt-line
--------------------------------------------------------------------------------
local function pkt_line(text)
  if text == "FLUSH"        then return "0000" end
  if text == "DELIM"        then return "0001" end
  if text == "RESPONSE_END" then return "0002" end
  local len = #text + 4
  return string.format("%04x%s", len, text)
end

--------------------------------------------------------------------------------
-- Utility to parse a stream of pkt-lines
--------------------------------------------------------------------------------
local function parse_pkt_lines(raw_data)
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
-- HTTP helper
--------------------------------------------------------------------------------
local function http_request(url, post_data, headers)
  local req = internet.request(url, post_data, headers)
  local chunks = {}
  for chunk in req do chunks[#chunks + 1] = chunk end
  return table.concat(chunks)
end

--------------------------------------------------------------------------------
-- Step 1: Discover available refs (ls-refs)
--
-- Protocol v2 works in two stages:
--   GET  /info/refs?service=git-upload-pack   → capability advertisement only
--   POST /git-upload-pack  with ls-refs cmd   → actual ref listing
--------------------------------------------------------------------------------
local function discover_refs(remote_url)
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
local function negotiate_packfile(remote_url, want_sha)
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
local function demux_sideband(response)
  local lines  = parse_pkt_lines(response)
  local chunks = {}

  for _, line in ipairs(lines) do
    if line ~= "FLUSH" and line ~= "DELIM" and line ~= "RESPONSE_END" then
      local channel = line:byte(1)
      local payload = line:sub(2)

      if channel == 1 then
        chunks[#chunks + 1] = payload          -- packfile data
      elseif channel == 2 then
        io.write(payload)                       -- progress (e.g. "Compressing…")
      elseif channel == 3 then
        error("Remote error: " .. payload)
      end
    end
  end

  return table.concat(chunks)
end

local function safe_inflate(bytes)
  local result
  repeat
    local ok, res, err_msg = pcall(data_comp.inflate, bytes)
    if not ok then error(res) end                              -- inflate threw
    if res == nil then
      if err_msg then
        error("inflate failed: " .. tostring(err_msg))        -- genuine error, don't retry
      end
      os.sleep(0.5)                                           -- no error msg = energy shortage, retry
    else
      result = res
    end
  until result ~= nil
  return result
end

local function inflate_at(data, pos, expected_size)
  -- Clamp the initial window: compressed size is almost always <= uncompressed,
  -- but add headroom for zlib header/trailer and edge cases.
  local lo = 6                          -- absolute minimum zlib stream
  local hi = math.min(
    math.max(expected_size + 512, 256), -- generous upper bound
    #data - pos + 1                     -- can't exceed remaining data
  )

  -- Find the smallest suffix [pos .. pos+N-1] that inflates successfully.
  -- This gives us the exact compressed length without any stream-parsing.
  local best_result = nil
  local best_len    = nil

  -- First confirm the full window actually works
  local full = safe_inflate(data:sub(pos, pos + hi - 1))
  assert(full and #full == expected_size,
    string.format("inflate_at: expected %d bytes, got %s", expected_size, tostring(full and #full)))

  best_result = full
  best_len    = hi

  -- Binary search downward to find the true stream boundary
  while lo < hi do
    local mid = math.floor((lo + hi) / 2)
    local ok, res = pcall(safe_inflate, data:sub(pos, pos + mid - 1))
    if ok and res and #res == expected_size then
      best_result = res
      best_len    = mid
      hi          = mid
    else
      lo = mid + 1
    end
  end

  return best_result, pos + best_len
end

--------------------------------------------------------------------------------
-- Write a loose object to .git/objects/xx/yyyy…
--------------------------------------------------------------------------------
local function write_object(git_dir, sha, type_name, content)
  local obj_dir  = git_dir .. "/objects/" .. sha:sub(1, 2)
  local obj_path = (git_dir:sub(1,1) ~= "/" and "/" or "") .. git_dir
                 .. "/objects/" .. sha:sub(1,2) .. "/" .. sha:sub(3)

  if filesystem.exists(obj_path) then return end

  if not filesystem.isDirectory(obj_dir) then
    local ok, err = filesystem.makeDirectory(obj_dir)
    if not ok then
      error("failed to create object dir " .. obj_dir .. ": " .. tostring(err))
    end
  end

  local store      = type_name .. " " .. #content .. "\0" .. content
  local compressed = data_comp.deflate(store)

  local f, err = io.open(obj_path, "wb")
  if not f then
    error("failed to open object file " .. obj_path .. ": " .. tostring(err))
  end
  f:write(compressed)
  f:close()
end

--------------------------------------------------------------------------------
-- Step 4a: Apply a binary delta (both OFS_DELTA and REF_DELTA share the same
-- delta-data format after the header has been stripped).
--
-- Delta format:
--   [source size varint] [target size varint]
--   Then a sequence of commands:
--     bit7=1  COPY   – copy a region from the base object
--     bit7=0  INSERT – copy N literal bytes from the delta stream
--------------------------------------------------------------------------------
local function apply_delta(base, delta)
  local pos = 1

  -- Read a variable-length integer (little-endian, MSB = continue)
  local function read_varint()
    local val, shift = 0, 0
    repeat
      local b = delta:byte(pos); pos = pos + 1
      val = val | ((b & 0x7F) << shift)
      shift = shift + 7
      if b & 0x80 == 0 then break end
    until false
    return val
  end

  local src_size = read_varint()
  local dst_size = read_varint()   -- used for sanity-checking below

  assert(#base == src_size,
    string.format("Delta base size mismatch: expected %d, got %d", src_size, #base))

  local result = {}

  while pos <= #delta do
    local cmd = delta:byte(pos); pos = pos + 1

    if cmd & 0x80 ~= 0 then
      ------------------------------------------------------------------
      -- COPY instruction
      -- Bits 0-3 of cmd select which of the next 4 bytes carry the offset.
      -- Bits 4-6 of cmd select which of the next 3 bytes carry the size.
      ------------------------------------------------------------------
      local copy_offset = 0
      local copy_size   = 0

      if cmd & 0x01 ~= 0 then copy_offset = copy_offset | (delta:byte(pos) <<  0); pos = pos + 1 end
      if cmd & 0x02 ~= 0 then copy_offset = copy_offset | (delta:byte(pos) <<  8); pos = pos + 1 end
      if cmd & 0x04 ~= 0 then copy_offset = copy_offset | (delta:byte(pos) << 16); pos = pos + 1 end
      if cmd & 0x08 ~= 0 then copy_offset = copy_offset | (delta:byte(pos) << 24); pos = pos + 1 end

      if cmd & 0x10 ~= 0 then copy_size   = copy_size   | (delta:byte(pos) <<  0); pos = pos + 1 end
      if cmd & 0x20 ~= 0 then copy_size   = copy_size   | (delta:byte(pos) <<  8); pos = pos + 1 end
      if cmd & 0x40 ~= 0 then copy_size   = copy_size   | (delta:byte(pos) << 16); pos = pos + 1 end

      if copy_size == 0 then copy_size = 0x10000 end  -- size=0 means 65536

      result[#result + 1] = base:sub(copy_offset + 1, copy_offset + copy_size)

    elseif cmd ~= 0 then
      ------------------------------------------------------------------
      -- INSERT instruction: cmd = number of literal bytes to copy
      ------------------------------------------------------------------
      result[#result + 1] = delta:sub(pos, pos + cmd - 1)
      pos = pos + cmd

    else
      error("Unexpected delta command byte 0x00")
    end
  end

  local out = table.concat(result)
  assert(#out == dst_size,
    string.format("Delta output size mismatch: expected %d, got %d", dst_size, #out))
  return out
end

--------------------------------------------------------------------------------
-- Step 4b: Parse the Packfile
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

local function parse_packfile(pack_data, git_dir)
  -- ── Header ────────────────────────────────────────────────────────────────
  assert(pack_data:sub(1, 4) == "PACK", "Invalid packfile magic")
  local version     = read_u32_be(pack_data, 5)
  local num_objects = read_u32_be(pack_data, 9)
  print(string.format("Packfile v%d – %d objects", version, num_objects))

  local pos = 13  -- byte position in pack_data (1-indexed)

  -- Tables to accumulate results
  local objects    = {}   -- sha  -> { type=string, data=string }
  local off_to_sha = {}   -- file_offset -> sha  (for OFS_DELTA resolution)
  local ofs_queue  = {}   -- pending OFS_DELTA entries
  local ref_queue  = {}   -- pending REF_DELTA entries

  -- ── Object loop ───────────────────────────────────────────────────────────
  for _ = 1, num_objects do
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

    if obj_type == 6 then
      -- ── OFS_DELTA: base is at (obj_start - offset) ──────────────────────
      -- The offset is encoded with a "bijective base-128" scheme
      byte = pack_data:byte(pos); pos = pos + 1
      local offset = byte & 0x7F
      while byte & 0x80 ~= 0 do
        byte   = pack_data:byte(pos); pos = pos + 1
        offset = ((offset + 1) << 7) | (byte & 0x7F)
      end

      local delta_data, new_pos = inflate_at(pack_data, pos, size)
      pos = new_pos

      ofs_queue[#ofs_queue + 1] = {
        obj_offset  = obj_start,
        base_offset = obj_start - offset,
        delta       = delta_data,
      }

    elseif obj_type == 7 then
      -- ── REF_DELTA: base identified by its 20-byte binary SHA ─────────────
      local base_sha = ""
      for j = 0, 19 do
        base_sha = base_sha .. string.format("%02x", pack_data:byte(pos + j))
      end
      pos = pos + 20

      local delta_data, new_pos = inflate_at(pack_data, pos, size)
      pos = new_pos

      ref_queue[#ref_queue + 1] = {
        obj_offset = obj_start,
        base_sha   = base_sha,
        delta      = delta_data,
      }

    else
      -- ── Regular object (commit / tree / blob / tag) ──────────────────────
      local type_name            = TYPE_NAMES[obj_type]
      local content, new_pos     = inflate_at(pack_data, pos, size)
      pos                        = new_pos

      local store = type_name .. " " .. #content .. "\0" .. content
      local sha   = sha1(store)

      objects[sha]      = { type = type_name, data = content }
      off_to_sha[obj_start] = sha

      write_object(git_dir, sha, type_name, content)
      print(string.format("  [%s] %s", type_name, sha))
    end
  end

  -- ── Delta resolution (multiple passes for chained deltas) ─────────────────
  local function resolve_pass()
    local resolved = 0

    -- OFS_DELTA
    local remaining_ofs = {}
    for _, entry in ipairs(ofs_queue) do
      local base_sha = off_to_sha[entry.base_offset]
      local base_obj = base_sha and objects[base_sha]
      if base_obj then
        local content   = apply_delta(base_obj.data, entry.delta)
        local type_name = base_obj.type
        local store     = type_name .. " " .. #content .. "\0" .. content
        local sha       = sha1(store)

        objects[sha]               = { type = type_name, data = content }
        off_to_sha[entry.obj_offset] = sha

        write_object(git_dir, sha, type_name, content)
        print(string.format("  [%s/ofs_delta] %s", type_name, sha))
        resolved = resolved + 1
      else
        remaining_ofs[#remaining_ofs + 1] = entry
      end
    end
    ofs_queue = remaining_ofs

    -- REF_DELTA
    local remaining_ref = {}
    for _, entry in ipairs(ref_queue) do
      local base_obj = objects[entry.base_sha]
      if base_obj then
        local content   = apply_delta(base_obj.data, entry.delta)
        local type_name = base_obj.type
        local store     = type_name .. " " .. #content .. "\0" .. content
        local sha       = sha1(store)

        objects[sha]               = { type = type_name, data = content }
        off_to_sha[entry.obj_offset] = sha

        write_object(git_dir, sha, type_name, content)
        print(string.format("  [%s/ref_delta] %s", type_name, sha))
        resolved = resolved + 1
      else
        remaining_ref[#remaining_ref + 1] = entry
      end
    end
    ref_queue = remaining_ref

    return resolved
  end

  -- Keep resolving until nothing left (handles chained deltas)
  repeat until resolve_pass() == 0

  if #ofs_queue + #ref_queue > 0 then
    print(string.format("WARNING: %d delta(s) could not be resolved (missing base objects)",
      #ofs_queue + #ref_queue))
  end

  return objects
end

--------------------------------------------------------------------------------
-- Checkout: walk tree objects and write blobs to the working directory
--------------------------------------------------------------------------------
local function checkout(git_dir, work_dir, tree_sha, objects)
  local tree_obj = objects[tree_sha]
  if not tree_obj then
    error("Tree object not found: " .. tree_sha)
  end

  local data = tree_obj.data
  local pos  = 1

  while pos <= #data do
    -- "mode name\0<20-byte-sha>"
    local space = data:find(" ", pos, true)
    local mode  = data:sub(pos, space - 1)
    pos = space + 1

    local null = data:find("\0", pos, true)
    local name = data:sub(pos, null - 1)
    pos = null + 1

    local sha = ""
    for i = 0, 19 do
      sha = sha .. string.format("%02x", data:byte(pos + i))
    end
    pos = pos + 20

    local full_path = work_dir .. "/" .. name

    if mode == "40000" or mode == "040000" then
      -- Sub-tree (directory)
      if not filesystem.isDirectory(full_path) then
        filesystem.makeDirectory(full_path)
      end
      checkout(git_dir, full_path, sha, objects)
    else
      -- Blob (file)
      local blob = objects[sha]
      if blob then
        local f = io.open(full_path, "wb")
        f:write(blob.data)
        f:close()
      else
        print("  WARNING: blob not found for " .. full_path .. " (" .. sha .. ")")
      end
    end
  end
end

--------------------------------------------------------------------------------
-- clone(remote_url, target_dir)
--   The main entry point – ties everything together.
--------------------------------------------------------------------------------
local function clone(remote_url, target_dir)
  -- 1. Discover refs
  local refs = discover_refs(remote_url)

  -- Pick HEAD: prefer main, then master, then any other branch
  local head_ref, head_sha
  for _, candidate in ipairs({ "refs/heads/main", "refs/heads/master" }) do
    if refs[candidate] then
      head_ref  = candidate
      head_sha  = refs[candidate]
      break
    end
  end
  if not head_sha then
    for ref, sha in pairs(refs) do
      head_ref, head_sha = ref, sha; break
    end
  end
  assert(head_sha, "No refs found on remote")

  local branch = head_ref:match("refs/heads/(.+)")
  print(string.format("HEAD → %s (%s)", branch, head_sha))

  -- 2. Create local directory structure
  local git_dir = target_dir .. "/.git"
  for _, d in ipairs({
    target_dir,
    git_dir,
    git_dir .. "/objects",
    git_dir .. "/refs",
    git_dir .. "/refs/heads",
  }) do
    if not filesystem.isDirectory(d) then filesystem.makeDirectory(d) end
  end

  -- 3. Fetch packfile
  local response = negotiate_packfile(remote_url, head_sha)

  -- 4. Demux sideband
  print("Demultiplexing sideband...")
  local pack_data = demux_sideband(response)
  print(string.format("Received %d bytes of packfile data", #pack_data))

  -- 5. Parse packfile (writes loose objects to .git/objects/)
  print("Parsing packfile...")
  local objects = parse_packfile(pack_data, git_dir)

  -- 6. Write .git/HEAD and the branch ref
  local head_f = io.open(git_dir .. "/HEAD", "w")
  head_f:write("ref: " .. head_ref .. "\n")
  head_f:close()

  local ref_f = io.open(git_dir .. "/" .. head_ref, "w")
  ref_f:write(head_sha .. "\n")
  ref_f:close()

  -- 7. Checkout working tree
  local commit_obj = objects[head_sha]
  assert(commit_obj, "HEAD commit not found in packfile: " .. head_sha)

  local tree_sha = commit_obj.data:match("^tree ([0-9a-f]+)")
  assert(tree_sha, "Could not find tree SHA in commit object")

  print("Checking out tree " .. tree_sha .. " → " .. target_dir)
  checkout(git_dir, target_dir, tree_sha, objects)

  print("Done! Repository cloned to " .. target_dir)
end

return {
  clone          = clone,
  sha1           = sha1,
  discover_refs  = discover_refs,
  demux_sideband = demux_sideband,
  parse_packfile = parse_packfile,
  apply_delta    = apply_delta,
}