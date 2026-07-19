-- pull.lua - git pull for OpenOS
--
-- Entry point: require this file and call M.pull(target_dir).
--
-- Pull is essentially:
--   1. Read .git/HEAD to find the current branch
--   2. Read .git/refs/heads/<branch> to find the local commit SHA
--   3. Read .git/config to find the remote URL (written by clone.lua)
--   4. discover_refs() on the remote
--   5. Find the remote SHA for the same branch
--   6. If local == remote: "Already up to date."
--   7. negotiate_packfile(want=remote_sha, have={local_sha})
--   8. Demux sideband, parse packfile → writes loose objects to .git/objects/
--   9. Walk the new tree: overwrite changed files, delete files removed on remote
--   10. Update .git/refs/heads/<branch> and refs/remotes/origin/<branch>

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
local read_loose_object = checkout_mod.read_loose_object

local util = require("util")
local sha1 = util.sha1

local M = {}

--------------------------------------------------------------------------------
-- Tiny INI-ish parser for .git/config.
-- Returns a nested table:  config[section_name][key] = value (string).
-- Section names are lower-cased and include any subsection in quotes, e.g.
--   remote "origin"   →  config['remote "origin"']
-- Values are stripped of surrounding whitespace; we do not handle multi-line
-- values (git's config doesn't use them for the keys we care about).
--------------------------------------------------------------------------------
local function parse_git_config(text)
  local config = {}
  local current_section
  for line in text:gmatch("[^\r\n]+") do
    local sec = line:match("^%s*%[%s*(.-)%s*%]%s*$")
    if sec then
      current_section = sec
      config[current_section] = config[current_section] or {}
    else
      local key, val = line:match("^%s*(%S-)%s*=%s*(.-)%s*$")
      if key and val and current_section then
        -- Strip surrounding quotes from value if present
        val = val:gsub("^\"(.-)\"$", "%1")
        config[current_section][key] = val
      end
    end
  end
  return config
end

--------------------------------------------------------------------------------
-- Read a UTF-8 text file and return its contents, or nil on failure.
--------------------------------------------------------------------------------
local function read_file(path)
  local f = io.open(path, "r")
  if not f then return nil end
  local s = f:read("*a")
  f:close()
  return s
end

--------------------------------------------------------------------------------
-- Read .git/HEAD and return (branch_name, head_ref) or (nil, nil) if HEAD
-- is in detached state (a raw SHA rather than "ref: refs/heads/...").
--------------------------------------------------------------------------------
local function read_head(git_dir)
  local head = read_file(git_dir .. "/HEAD")
  if not head then return nil, nil end
  head = head:gsub("^%s+", ""):gsub("%s+$", "")
  local ref = head:match("^ref:%s*(refs/heads/.+)$")
  if not ref then return nil, nil end  -- detached HEAD
  local branch = ref:match("refs/heads/(.+)$")
  return branch, ref
end

--------------------------------------------------------------------------------
-- Read a ref file (e.g. .git/refs/heads/main) and return the SHA it points
-- to (lower-cased, trimmed).  Returns nil if the file does not exist.
--------------------------------------------------------------------------------
local function read_ref(git_dir, ref_path)
  local s = read_file(git_dir .. "/" .. ref_path)
  if not s then return nil end
  s = s:gsub("^%s+", ""):gsub("%s+$", "")
  -- Refs can be symbolic ("ref: refs/heads/foo"); follow one level.
  local sym = s:match("^ref:%s*(.+)$")
  if sym then return read_ref(git_dir, sym) end
  return s:lower()
end

--------------------------------------------------------------------------------
-- Read .git/config and return the remote URL for the given remote name
-- (defaults to "origin").  Returns nil if not found.
--------------------------------------------------------------------------------
local function read_remote_url(git_dir, remote_name)
  remote_name = remote_name or "origin"
  local text = read_file(git_dir .. "/config")
  if not text then return nil end
  local config = parse_git_config(text)
  local section = config['remote "' .. remote_name .. '"']
  if not section then return nil end
  return section.url
end

--------------------------------------------------------------------------------
-- Read a commit object and return its tree SHA (40-char lowercase hex).
-- Accepts either the in-RAM objects table or a loose object on disk.
--------------------------------------------------------------------------------
local function commit_tree_sha(git_dir, commit_sha, objects)
  local obj = objects and objects[commit_sha]
  local data = obj and obj.data
  if not data then
    data = read_loose_object(git_dir, commit_sha)
  end
  if not data then return nil end
  local tree = data:match("^tree ([0-9a-f]+)")
  return tree and tree:lower()
end

--------------------------------------------------------------------------------
-- Walk a tree object recursively and call fn(path, mode, sha) for every entry.
--   path  – relative path from the work dir root (uses "/" separators)
--   mode  – git tree entry mode ("40000" for trees, "100644"/"100755"/"120000"…)
--   sha   – 40-char lowercase hex of the entry's object
-- Directories are descended into automatically; fn is NOT called for them.
--------------------------------------------------------------------------------
local function walk_tree(git_dir, tree_sha, prefix, objects, fn)
  local obj = objects[tree_sha]
  local data = obj and obj.data
  if not data then
    data = read_loose_object(git_dir, tree_sha)
  end
  if not data then
    error("walk_tree: tree object not found: " .. tree_sha)
  end

  local pos = 1
  while pos <= #data do
    local space = data:find(" ", pos, true)
    if not space then break end
    local mode = data:sub(pos, space - 1)
    pos = space + 1

    local nul = data:find("\0", pos, true)
    if not nul then break end
    local name = data:sub(pos, nul - 1)
    pos = nul + 1

    local sha = ""
    for i = 0, 19 do
      sha = sha .. string.format("%02x", data:byte(pos + i))
    end
    pos = pos + 20

    local full_path = prefix == "" and name or (prefix .. "/" .. name)

    if mode == "40000" or mode == "040000" then
      walk_tree(git_dir, sha, full_path, objects, fn)
    else
      fn(full_path, mode, sha:lower())
    end
  end
end

--------------------------------------------------------------------------------
-- Build a flat map of  path -> sha  for every blob under the given tree.
-- Used to compute the set of files that must be deleted after a pull (those
-- present in the old tree but absent from the new one).
--------------------------------------------------------------------------------
local function collect_tree_files(git_dir, tree_sha, objects)
  local files = {}
  walk_tree(git_dir, tree_sha, "", objects, function(path, _mode, sha)
    files[path] = sha
  end)
  return files
end

--------------------------------------------------------------------------------
-- pull(target_dir)
--   The main entry point.
--
--   target_dir defaults to the current working directory.
--------------------------------------------------------------------------------
function M.pull(target_dir)
  dbg_enter("pull")

  -- Default to CWD
  if not target_dir then
    local shell = require("shell")
    target_dir = shell.resolve(".")
  end
  if target_dir:sub(1,1) ~= "/" then
    local shell = require("shell")
    target_dir = shell.resolve(target_dir)
  end

  local git_dir = target_dir .. "/.git"
  if not filesystem.isDirectory(git_dir) then
    error("not a git repository: " .. target_dir .. " (no .git directory)")
  end

  -- 1. Read current branch + local commit SHA
  local branch, head_ref = read_head(git_dir)
  if not branch then
    error("pull is only supported on a branch (HEAD is detached or missing)")
  end
  dbg("pull: branch=%s head_ref=%s", branch, head_ref)

  local local_sha = read_ref(git_dir, head_ref)
  if not local_sha then
    error("could not read local ref " .. head_ref .. " – nothing to pull from")
  end
  print(string.format("Current branch: %s (%s)", branch, local_sha))

  -- 2. Read remote URL from .git/config
  local remote_url = read_remote_url(git_dir, "origin")
  if not remote_url then
    error("no remote 'origin' configured in .git/config")
  end
  print(string.format("Remote: %s", remote_url))
  dbg("pull: remote_url=%s", remote_url)

  if inflate.cpu_deflate then
    dbg("pull: inflate strategy = cpu_deflate (pure-Lua)")
  else
    dbg("pull: inflate strategy = datacard component")
  end

  -- 3. Discover remote refs
  local refs = discover_refs(remote_url)
  dbg("pull: discovered refs:")
  for ref, sha in pairs(refs) do
    dbg("  %s → %s", ref, sha)
  end

  -- 4. Find remote SHA for the same branch
  local remote_ref = "refs/heads/" .. branch
  local remote_sha = refs[remote_ref]
  if not remote_sha then
    -- Fall back to main / master if the remote branch matches neither
    for _, candidate in ipairs({ "refs/heads/main", "refs/heads/master" }) do
      if refs[candidate] then
        remote_ref = candidate
        remote_sha = refs[candidate]
        break
      end
    end
  end
  if not remote_sha then
    error("remote has no branch '" .. branch .. "' (and no main/master to fall back on)")
  end
  remote_sha = remote_sha:lower()
  print(string.format("Remote %s → %s", branch, remote_sha))

  -- 5. Already up to date?
  if remote_sha == local_sha then
    print("Already up to date.")
    dbg_leave("pull")
    return
  end

  -- 6. Decide between a delta fetch and a full fetch.
  --
  -- A delta fetch advertises local_sha as a 'have' so the server omits
  -- objects the client already has.  This ONLY works if those objects are
  -- actually present on disk (or in RAM) — otherwise walk_tree will fail
  -- on the first unchanged subtree that isn't in the delta pack.
  --
  -- Legacy clones (created before this fix) did not persist objects to
  -- disk during clone, so we detect that case by trying to read the local
  -- commit object from disk.  If it's missing, we do a full fetch (no
  -- 'have' lines), which causes the server to send every object reachable
  -- from remote_sha.  This is wasteful (it's effectively a re-clone) but
  -- it's the only way to recover a legacy clone without re-running clone.
  -- After this pull, .git/objects/ will be populated and subsequent pulls
  -- will use the cheap delta path.
  -- NOTE: it's not enough to check that the *commit* object is present –
  -- a delta fetch tells the server "I already have everything reachable
  -- from local_sha", so if even one subtree/blob under it is missing on
  -- disk, walk_tree() will crash later trying to read an object the
  -- server never sent. So we actually walk the old tree here to confirm
  -- the whole graph is intact before committing to a delta fetch.
  local local_commit_data = read_loose_object(git_dir, local_sha)
  local have_shas
  local old_tree_intact = false
  if local_commit_data then
    local old_tree_probe = local_commit_data:match("^tree ([0-9a-f]+)")
    if old_tree_probe then
      local ok_probe = pcall(walk_tree, git_dir, old_tree_probe:lower(), "", {}, function() end)
      old_tree_intact = ok_probe
    end
  end

  if old_tree_intact then
    print(string.format("Fetching updates %s → %s ...",
        local_sha:sub(1,7), remote_sha:sub(1,7)))
    dbg("pull: local commit %s and full tree found on disk – using delta fetch", local_sha)
    have_shas = { local_sha }
  else
    print(string.format("Local state incomplete (objects missing from disk);"))
    print(string.format("doing a full fetch from %s ...", remote_sha:sub(1,7)))
    print(string.format("(This is a one-time cost – future pulls will be fast.)"))
    dbg("pull: local commit %s incomplete on disk – using full fetch", local_sha)
    have_shas = nil
  end

  local response = negotiate_packfile(remote_url, remote_sha, have_shas)
  dbg("pull: raw server response=%d bytes", #response)

  -- 7. Demux sideband (may be empty if server says "already up to date"
  --    because the client has every reachable object)
  local pack_data = demux_sideband(response)

  local objects = {}
  if #pack_data >= 12 and pack_data:sub(1, 4) == "PACK" then
    print(string.format("Received %d bytes of packfile data", #pack_data))
    dbg_hex("pull: packfile header", pack_data, 12)

    -- 8. Parse packfile → writes loose objects to .git/objects/
    print("Parsing packfile...")
    objects = parse_packfile(pack_data, git_dir)
  else
    -- Server sent no pack data (can happen if local already has the new tip
    -- via a previous partial fetch).  Continue with whatever is on disk.
    print("No new packfile data – server reports client is up to date.")
    dbg("pull: demux produced %d bytes, no PACK magic – skipping parse", #pack_data)
  end

  -- 9. Synchronise the working tree
  --
  -- Strategy:
  --   a) Build a flat path→sha map of the OLD tree (from local_sha's commit)
  --   b) Walk the NEW tree (from remote_sha's commit) and:
  --        - write every blob (overwrite if changed)
  --        - remove the path from the old map (so remaining entries are deletions)
  --   c) Delete every file still in the old map
  --
  -- We re-checkout every blob unconditionally rather than diffing SHAs because
  -- the working tree may have been touched locally (OCgit has no index, so we
  -- cannot rely on it).  This is energy-expensive but simple and correct.
  local old_tree_sha = commit_tree_sha(git_dir, local_sha, objects)
  local new_tree_sha = commit_tree_sha(git_dir, remote_sha, objects)
  if not new_tree_sha then
    error("could not read tree SHA from remote commit " .. remote_sha)
  end
  dbg("pull: old_tree=%s new_tree=%s", tostring(old_tree_sha), new_tree_sha)

  print("Updating working tree...")

  local old_files = {}
  if old_tree_sha then
    -- Try to walk the old tree to build the deletion set.  If any subtree
    -- is missing (e.g. legacy clone that didn't persist objects, OR a
    -- subtree that was deleted on the remote and thus not in the full
    -- fetch), skip the deletion step gracefully rather than crashing.
    -- The NEW tree walk (below) is independent and will still succeed.
    local ok_old, result_or_err = pcall(collect_tree_files, git_dir, old_tree_sha, objects)
    if ok_old then
      old_files = result_or_err
    else
      print("WARNING: could not walk old tree (" .. tostring(result_or_err) .. ")")
      print("         file deletions will not be applied this run.")
      print("         (This is expected for legacy clones; future pulls will work fully.)")
      old_files = {}
    end
  end

  local written   = 0
  local unchanged = 0
  local ok_new_walk, new_walk_err = pcall(walk_tree, git_dir, new_tree_sha, "", objects, function(rel_path, mode, sha)
    local abs_path = target_dir .. "/" .. rel_path

    -- Ensure parent directory exists
    local parent = filesystem.path(abs_path)
    if parent and not filesystem.isDirectory(parent) then
      filesystem.makeDirectory(parent)
    end

    -- Look up blob content (RAM first, then disk)
    local obj = objects[sha]
    local content = obj and obj.data
    if not content then
      content = read_loose_object(git_dir, sha)
    end
    if not content then
      io.write("  WARNING: blob missing for " .. rel_path .. " (" .. sha .. ")\n")
      old_files[rel_path] = nil
      return
    end

    -- Write (overwrite) the file
    local f, err = io.open(abs_path, "wb")
    if not f then
      io.write("  WARNING: could not write " .. rel_path .. ": " .. tostring(err) .. "\n")
      old_files[rel_path] = nil
      return
    end
    f:write(content)
    f:close()

    -- Preserve executable bit if mode says so (OC filesystem is mostly
    -- modeless, but try anyway – harmlessly fails on read-only FSes).
    if mode == "100755" then
      pcall(filesystem.setExecutable, abs_path, true)
    end

    if old_files[rel_path] == sha then
      unchanged = unchanged + 1
    else
      written = written + 1
      print("  Updated: " .. rel_path)
    end
    old_files[rel_path] = nil
  end)
  if not ok_new_walk then
    error("pull: failed to walk the new tree (" .. tostring(new_walk_err) .. ").\n" ..
          "This usually means the local repo is missing objects the server\n" ..
          "assumed you already had. Try deleting .git/objects and re-running\n" ..
          "pull to force a full re-fetch, or re-clone the repository.")
  end

  -- 10. Delete files that vanished on the remote
  local deleted = 0
  for rel_path, _ in pairs(old_files) do
    local abs_path = target_dir .. "/" .. rel_path
    print("  Removed: " .. rel_path)
    pcall(filesystem.remove, abs_path)
    deleted = deleted + 1

    -- Best-effort: prune now-empty parent directories.
    -- Note: OpenOS filesystem.list() returns an iterator, not a table.
    local parent = filesystem.path(abs_path)
    while parent and parent ~= target_dir and parent ~= "/" do
      local is_empty = true
      for _ in filesystem.list(parent) do
        is_empty = false
        break
      end
      if is_empty then
        pcall(filesystem.remove, parent)
        parent = filesystem.path(parent)
      else
        break
      end
    end
  end

  -- Prune empty directories left behind in the new tree as well
  -- (git would normally leave "empty" dirs alone, but OC has no .gitkeep
  -- convention; we leave them – harmless).

  print(string.format("Summary: %d updated, %d unchanged, %d removed",
      written, unchanged, deleted))

  -- 11. Update the local ref
  local ref_path = git_dir .. "/" .. head_ref
  local rf = io.open(ref_path, "w")
  assert(rf, "could not open " .. ref_path .. " for writing")
  rf:write(remote_sha .. "\n")
  rf:close()
  dbg("pull: updated %s → %s", head_ref, remote_sha)

  -- Update remote-tracking ref if it exists
  local remote_ref_path = git_dir .. "/refs/remotes/origin/" .. branch
  local rrf = io.open(remote_ref_path, "w")
  if rrf then
    rrf:write(remote_sha .. "\n")
    rrf:close()
  end

  -- Update HEAD if it was a symbolic ref (it always is in our case)
  -- No-op: HEAD already says "ref: refs/heads/<branch>", and we just
  -- updated that ref file.

  print(string.format("Done!  %s is now at %s", branch, remote_sha))
  dbg_leave("pull")
end

-- Re-export internal helpers for unit tests / external callers.
M.read_head          = read_head
M.read_ref           = read_ref
M.read_remote_url    = read_remote_url
M.parse_git_config   = parse_git_config
M.commit_tree_sha    = commit_tree_sha
M.walk_tree          = walk_tree
M.collect_tree_files = collect_tree_files

return M