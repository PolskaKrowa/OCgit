-- packfile.lua
-- Packfile parsing, loose object writing, and delta application.
--
--   write_object    – compress and store a loose object under .git/objects/
--   apply_delta     – reconstruct an object from a binary delta and its base
--   parse_packfile  – parse a full PACK stream into an objects table

local filesystem = require("filesystem")

local D       = require("debug")
local dbg       = D.dbg
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
-- write_object_now
-- Compress and immediately store one loose object under .git/objects/.
-- Uses the datacard deflate, so call sparingly (it is energy-expensive).
--------------------------------------------------------------------------------
function M.write_object_now(git_dir, sha, type_name, content)
  local obj_dir  = git_dir .. "/objects/" .. sha:sub(1, 2)
  local obj_path = (git_dir:sub(1,1) ~= "/" and "/" or "") .. git_dir
                 .. "/objects/" .. sha:sub(1,2) .. "/" .. sha:sub(3)

  if filesystem.exists(obj_path) then
    dbg("write_object_now: skipping %s (already on disk)", sha)
    return
  end

  if not filesystem.isDirectory(obj_dir) then
    local ok, err = filesystem.makeDirectory(obj_dir)
    if not ok then
      error("failed to create object dir " .. obj_dir .. ": " .. tostring(err))
    end
  end

  dbg("write_object_now: deflating and writing %s %s (%d bytes)", type_name, sha, #content)
  local store      = type_name .. " " .. #content .. "\0" .. content
  local compressed = safe_deflate(store)

  local f, err = io.open(obj_path, "wb")
  if not f then
    error("failed to open object file " .. obj_path .. ": " .. tostring(err))
  end
  f:write(compressed)
  f:close()
end

--------------------------------------------------------------------------------
-- write_object
-- No-op stub kept so that parse_packfile call-sites compile unchanged.
-- Objects are held in RAM; checkout will call write_object_now on-demand
-- for any object it cannot find in the in-memory table.
-- Flushing every object via safe_deflate() during parsing causes energy
-- starvation on the datacard (hundreds of calls back-to-back).
--------------------------------------------------------------------------------
function M.write_object(git_dir, sha, type_name, content)
  dbg("write_object: deferred (RAM-only) %s %s", type_name, sha)
end

--------------------------------------------------------------------------------
-- Apply a binary delta (both OFS_DELTA and REF_DELTA share this format
-- once the outer header has been stripped).
--
-- Delta format:
--   [source size varint] [target size varint]
--   Then a sequence of commands:
--     bit7=1  COPY   – copy a region from the base object
--     bit7=0  INSERT – copy N literal bytes from the delta stream
--------------------------------------------------------------------------------
function M.apply_delta(base, delta)
  dbg_enter("apply_delta")
  dbg("apply_delta: base_size=%d delta_size=%d", #base, #delta)

  if D.DEBUG_DELTA then
    local out = {}
    for i = 1, math.min(#delta, 16) do
      out[#out + 1] = string.format("%02x", delta:byte(i))
    end
    dbg("apply_delta: delta header bytes: %s", table.concat(out, " "))
  end

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
  local dst_size = read_varint()

  dbg("apply_delta: src_size=%d (base=%d) dst_size=%d varint_header_end_pos=%d",
      src_size, #base, dst_size, pos)

  if #base ~= src_size then
    dbg("apply_delta: ERROR – base size mismatch! base=%d src_size=%d", #base, src_size)
  end

  assert(#base == src_size,
    string.format("Delta base size mismatch: expected %d, got %d", src_size, #base))

  local result     = {}
  local result_len = 0
  local cmd_count  = 0
  local copy_count = 0
  local ins_count  = 0

  -- Yield every 32 commands so the OC energy capacitor can recharge.
  -- apply_delta is pure Lua but large blobs can have thousands of COPY
  -- instructions; without yielding the capacitor drains mid-loop.
  local YIELD_EVERY = 16

  while pos <= #delta do
    local cmd = delta:byte(pos); pos = pos + 1
    cmd_count = cmd_count + 1
    if cmd_count % YIELD_EVERY == 0 then os.sleep(0.5) end

    if cmd & 0x80 ~= 0 then
      ------------------------------------------------------------
      -- COPY instruction
      -- Bits 0-3 of cmd select which of the next 4 bytes carry the offset.
      -- Bits 4-6 of cmd select which of the next 3 bytes carry the size.
      ------------------------------------------------------------
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

      if D.DEBUG_DELTA then
        dbg("apply_delta: cmd#%d COPY offset=%d size=%d (base_end=%d out_so_far=%d)",
            cmd_count, copy_offset, copy_size, copy_offset + copy_size, result_len)
      end

      -- Bounds check before slicing
      if copy_offset + copy_size > #base then
        dbg("apply_delta: ERROR – COPY out of bounds! offset=%d size=%d base_size=%d",
            copy_offset, copy_size, #base)
        error(string.format("Delta COPY out of bounds: offset=%d size=%d base_size=%d",
              copy_offset, copy_size, #base))
      end

      result[#result + 1] = base:sub(copy_offset + 1, copy_offset + copy_size)
      result_len = result_len + copy_size
      copy_count = copy_count + 1

    elseif cmd ~= 0 then
      ------------------------------------------------------------
      -- INSERT instruction: cmd = number of literal bytes to copy
      ------------------------------------------------------------
      if D.DEBUG_DELTA then
        dbg("apply_delta: cmd#%d INSERT %d literal bytes (out_so_far=%d)",
            cmd_count, cmd, result_len)
      end

      -- Bounds check
      if pos + cmd - 1 > #delta then
        dbg("apply_delta: ERROR – INSERT reads past end of delta! pos=%d cmd=%d delta_size=%d",
            pos, cmd, #delta)
        error(string.format("Delta INSERT out of bounds: pos=%d len=%d delta_size=%d",
              pos, cmd, #delta))
      end

      result[#result + 1] = delta:sub(pos, pos + cmd - 1)
      result_len = result_len + cmd
      pos        = pos + cmd
      ins_count  = ins_count + 1

    else
      dbg("apply_delta: ERROR – unexpected 0x00 command byte at delta pos %d", pos - 1)
      error("Unexpected delta command byte 0x00")
    end
  end

  local out = table.concat(result)

  dbg("apply_delta: finished – %d cmds (%d COPY, %d INSERT), output=%d bytes expected=%d",
      cmd_count, copy_count, ins_count, #out, dst_size)

  if #out ~= dst_size then
    dbg("apply_delta: ERROR – output size mismatch! got=%d expected=%d", #out, dst_size)
  end

  assert(#out == dst_size,
    string.format("Delta output size mismatch: expected %d, got %d", dst_size, #out))

  dbg_leave("apply_delta")
  return out
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
      os.sleep(0)
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
        resolved = resolved + 1
      else
        remaining_ofs[#remaining_ofs + 1] = entry
      end
    end
    ofs_queue = remaining_ofs

    -- REF_DELTA
    local remaining_ref = {}
    for _, entry in ipairs(ref_queue) do
      os.sleep(0)
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
        resolved = resolved + 1
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