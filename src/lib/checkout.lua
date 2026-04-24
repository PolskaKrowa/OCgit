-- checkout.lua
-- Walk a git tree object and write blob contents to the working directory.
--
-- Objects are looked up in RAM first (the objects table populated by
-- parse_packfile), then fallen back to loose object files on disk.

local filesystem = require("filesystem")

local D   = require("debug")
local dbg = D.dbg

local inflate      = require("pack_inflate")
local safe_inflate = inflate.safe_inflate


local M = {}

--------------------------------------------------------------------------------
-- Read a loose object from .git/objects/xx/yyyy… and return its raw content
-- (the part after the "type size\0" header).
--------------------------------------------------------------------------------
function M.read_loose_object(git_dir, sha)
  local path = git_dir .. "/objects/" .. sha:sub(1, 2) .. "/" .. sha:sub(3)
  local f = io.open(path, "rb")
  if not f then return nil end
  local compressed = f:read("*a")
  f:close()

  local decompressed = safe_inflate(compressed)
  -- Git loose objects are: "type size\0content"
  local null_pos = decompressed:find("\0", 1, true)
  return decompressed:sub(null_pos + 1)
end

--------------------------------------------------------------------------------
-- Recursively walk a tree object, creating directories and writing blobs.
--
-- objects  – the in-RAM table from parse_packfile (may be partial; falls back
--            to disk for anything missing)
--------------------------------------------------------------------------------
function M.checkout(git_dir, work_dir, tree_sha, objects)
  dbg("checkout: tree_sha=%s work_dir=%s", tree_sha, work_dir)

  -- Try RAM first, then disk
  local from_ram  = objects[tree_sha] ~= nil
  local tree_obj  = objects[tree_sha]
  local tree_data = tree_obj and tree_obj.data
  if not tree_data then
    dbg("checkout: tree RAM miss for sha=%s, checking disk", tree_sha)
    tree_data = M.read_loose_object(git_dir, tree_sha)
  end

  if not tree_data then
    dbg("checkout: ERROR – tree object not found in RAM or disk: %s", tree_sha)
    error("Tree object not found: " .. tree_sha)
  end

  dbg("checkout: tree_data=%d bytes (source=%s)", #tree_data, from_ram and "RAM" or "disk")

  local pos         = 1
  local entry_count = 0

  while pos <= #tree_data do
    os.sleep(0)

    local space = tree_data:find(" ", pos, true)
    local mode  = tree_data:sub(pos, space - 1)
    pos = space + 1

    local null = tree_data:find("\0", pos, true)
    local name = tree_data:sub(pos, null - 1)
    pos = null + 1

    local sha = ""
    for i = 0, 19 do
      sha = sha .. string.format("%02x", tree_data:byte(pos + i))
    end
    pos = pos + 20
    entry_count = entry_count + 1

    local full_path = work_dir .. "/" .. name

    if mode == "40000" or mode == "040000" then
      dbg("checkout: entry#%d TREE mode=%s name=%s sha=%s", entry_count, mode, name, sha)
      if not filesystem.isDirectory(full_path) then
        filesystem.makeDirectory(full_path)
      end
      M.checkout(git_dir, full_path, sha, objects)
    else
      local in_ram = objects[sha] ~= nil
      dbg("checkout: entry#%d BLOB mode=%s name=%s sha=%s (source=%s)",
          entry_count, mode, name, sha, in_ram and "RAM" or "disk")
      print("  Writing: " .. name)
      local obj = objects[sha]
      local content = obj and obj.data
      if not content then
        -- Cache miss: object was evicted or never written; persist it now
        -- so read_loose_object can decompress it back.
        dbg("checkout: RAM miss for sha=%s, checking disk", sha)
        content = M.read_loose_object(git_dir, sha)
      end
      if content then
        dbg("checkout: writing %d bytes to %s", #content, full_path)
        local f = io.open(full_path, "wb")
        f:write(content)
        f:close()
      else
        -- Object missing from both RAM and disk - this is unrecoverable.
        -- During a fresh clone all objects should be in the RAM table;
        -- if this fires something went wrong in parse_packfile.
        error(string.format("checkout: blob not found in RAM or disk: sha=%s name=%s", sha, name))
      end
    end
  end

  dbg("checkout: done – %d entries processed in %s", entry_count, work_dir)
end

return M