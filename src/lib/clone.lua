-- clone.lua - A minimal git clone implementation for OpenOS
-- Entry point: require this file and call M.clone(remote_url, target_dir).
--
-- Module layout:
--   debug.lua    – DEBUG flags and dbg() helpers
--   util.lua     – sha1, pkt-line helpers, http_request
--   inflate.lua  – zlib inflate/deflate (cpu + datacard paths)
--   protocol.lua – discover_refs, negotiate_packfile, demux_sideband
--   packfile.lua – write_object, apply_delta, parse_packfile
--   checkout.lua – read_loose_object, checkout

local filesystem = require("filesystem")

local D        = require("debug")
local dbg       = D.dbg
local dbg_enter = D.dbg_enter
local dbg_leave = D.dbg_leave
local dbg_hex   = D.dbg_hex

local inflate  = require("pack_inflate")

local protocol = require("protocol")
local discover_refs      = protocol.discover_refs
local negotiate_packfile = protocol.negotiate_packfile
local demux_sideband     = protocol.demux_sideband

local packfile = require("packfile")
local parse_packfile = packfile.parse_packfile

local checkout_mod = require("checkout")
local checkout = checkout_mod.checkout

local M = {}

--------------------------------------------------------------------------------
-- clone(remote_url, target_dir)
--   The main entry point – ties everything together.
--------------------------------------------------------------------------------
function M.clone(remote_url, target_dir)
  dbg_enter("clone")
  dbg("clone: remote_url=%s target_dir=%s", remote_url, target_dir)

  if inflate.cpu_deflate then
    print("Using pure lua inflate")
    dbg("clone: inflate strategy = cpu_deflate (pure-Lua)")
  else
    print("Using datacard inflate")
    dbg("clone: inflate strategy = datacard component (binary search)")
  end

  -- 1. Discover refs
  local refs = discover_refs(remote_url)

  -- Log all discovered refs
  dbg("clone: discovered refs:")
  local ref_count = 0
  for ref, sha in pairs(refs) do
    ref_count = ref_count + 1
    dbg("  %s → %s", ref, sha)
  end
  dbg("clone: total refs=%d", ref_count)

  -- Pick HEAD: prefer main, then master, then any other branch
  local head_ref, head_sha
  for _, candidate in ipairs({ "refs/heads/main", "refs/heads/master" }) do
    if refs[candidate] then
      head_ref = candidate
      head_sha = refs[candidate]
      dbg("clone: selected HEAD via preferred candidate: %s", candidate)
      break
    end
  end
  if not head_sha then
    for ref, sha in pairs(refs) do
      head_ref, head_sha = ref, sha
      dbg("clone: no main/master found, falling back to first available ref: %s", ref)
      break
    end
  end
  assert(head_sha, "No refs found on remote")

  local branch = head_ref:match("refs/heads/(.+)")
  print(string.format("HEAD → %s (%s)", branch, head_sha))

  -- 2. Create local directory structure
  -- Resolve to an absolute path; io.open on OpenOS requires absolute paths.
  if target_dir:sub(1,1) ~= "/" then
    local shell = require("shell")
    target_dir = shell.resolve(target_dir)
    dbg("clone: resolved target_dir to absolute path: %s", target_dir)
  end
  local git_dir = target_dir .. "/.git"
  for _, d in ipairs({
    target_dir,
    git_dir,
    git_dir .. "/objects",
    git_dir .. "/refs",
    git_dir .. "/refs/heads",
  }) do
    if not filesystem.isDirectory(d) then
      local ok, err = filesystem.makeDirectory(d)
      dbg("clone: mkdir %s -> ok=%s err=%s", d, tostring(ok), tostring(err))
    end
  end

  -- 3. Fetch packfile
  dbg("clone: fetching packfile for sha=%s", head_sha)
  local response = negotiate_packfile(remote_url, head_sha)
  dbg("clone: raw server response=%d bytes", #response)

  -- 4. Demux sideband
  print("Demultiplexing sideband...")
  local pack_data = demux_sideband(response)
  print(string.format("Received %d bytes of packfile data", #pack_data))
  dbg("clone: sideband demux ratio: %.1f%% of response was pack data",
      (#pack_data / math.max(#response, 1)) * 100)
  dbg_hex("clone: packfile header", pack_data, 12)

  -- 5. Parse packfile (writes loose objects to .git/objects/)
  print("Parsing packfile...")
  local objects = parse_packfile(pack_data, git_dir)
  dbg("clone: packfile parsed, objects in RAM=%d",
      (function() local n=0 for _ in pairs(objects) do n=n+1 end return n end)())

  -- 6. Write .git/HEAD and the branch ref
  local head_path = git_dir .. "/HEAD"
  dbg("clone: writing .git/HEAD → %s  (full path: %s)", head_ref, head_path)
  local head_f, head_err = io.open(head_path, "w")
  assert(head_f, "Failed to open " .. head_path .. ": " .. tostring(head_err))
  head_f:write("ref: " .. head_ref .. "\n")
  head_f:close()

  local ref_path = git_dir .. "/" .. head_ref
  dbg("clone: writing ref file %s → %s  (full path: %s)", head_ref, head_sha, ref_path)
  local ref_f, ref_err = io.open(ref_path, "w")
  assert(ref_f, "Failed to open " .. ref_path .. ": " .. tostring(ref_err))
  ref_f:write(head_sha .. "\n")
  ref_f:close()

  -- 7. Checkout working tree
  local commit_obj = objects[head_sha]
  if not commit_obj then
    dbg("clone: ERROR – HEAD commit sha=%s not found in objects table", head_sha)
  end
  assert(commit_obj, "HEAD commit not found in packfile: " .. head_sha)
  dbg("clone: HEAD commit type=%s size=%d", commit_obj.type, #commit_obj.data)

  local tree_sha = commit_obj.data:match("^tree ([0-9a-f]+)")
  if not tree_sha then
    dbg("clone: ERROR – could not find 'tree <sha>' line in commit object data")
    dbg("clone: commit data (first 256 chars): %s", commit_obj.data:sub(1,256):gsub("[%c]","."))
  end
  assert(tree_sha, "Could not find tree SHA in commit object")
  dbg("clone: root tree_sha=%s", tree_sha)

  print("Checking out tree " .. tree_sha .. " → " .. target_dir)
  checkout(git_dir, target_dir, tree_sha, objects)

  print("Done! Repository cloned to " .. target_dir)
  dbg_leave("clone")
end

-- Re-export sub-module symbols for callers that accessed them via the old
-- single-file return table (e.g. unit tests).
M.sha1           = require("util").sha1
M.discover_refs  = discover_refs
M.demux_sideband = demux_sideband
M.parse_packfile = parse_packfile
M.apply_delta    = packfile.apply_delta

return M