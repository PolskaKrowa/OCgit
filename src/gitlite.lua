-- gitlite.lua
-- Minimal dumb-HTTP Git client for OpenComputers/OpenOS
-- Supports:
--   gitlite clone <url> [dir]
--   gitlite pull <dir>
--   gitlite sync <url> [dir]
--
-- Notes:
--   * Requires a Data Card for component.data.inflate()
--   * Works only with "dumb" HTTP Git repositories
--   * Does not support packfiles, smart HTTP, SSH, merges, rebase, or tags

local fs = require("filesystem")
local internet = require("internet")
local component = require("component")

local data = component.isAvailable("data") and component.data or nil
if not data then
  io.stderr:write("This script requires a Data Card for inflate support.\n")
  io.stderr:write("Install a Data Card, then try again.\n")
  os.exit(1)
end

local function trim(s)
  return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function trim_slashes(s)
  return (s:gsub("/+$", ""))
end

local function join_url(base, path)
  base = trim_slashes(base)
  path = tostring(path or ""):gsub("^/+", "")
  return base .. "/" .. path
end

local function hex_to_bin(hex)
  hex = hex:gsub("%s+", ""):lower()
  return (hex:gsub("..", function(cc)
    return string.char(tonumber(cc, 16))
  end))
end

local function bin_to_hex(bin)
  return (bin:gsub(".", function(c)
    return string.format("%02x", string.byte(c))
  end))
end

local function read_file(path)
  local f = io.open(path, "rb")
  if not f then return nil end
  local s = f:read("*a")
  f:close()
  return s
end

local function write_file(path, content)
  local parent = path:match("^(.*)/[^/]+$")
  if parent and parent ~= "" and not fs.exists(parent) then
    fs.makeDirectory(parent)
  end
  local f, err = io.open(path, "wb")
  if not f then
    return nil, err
  end
  f:write(content)
  f:close()
  return true
end

local function mkdir_p(path)
  if not path or path == "" then return true end
  if fs.exists(path) then return true end
  local parent = path:match("^(.*)/[^/]+$")
  if parent and parent ~= "" and not fs.exists(parent) then
    local ok, err = mkdir_p(parent)
    if not ok then return nil, err end
  end
  if not fs.exists(path) then
    local ok, err = pcall(fs.makeDirectory, path)
    if not ok then
      return nil, err
    end
  end
  return true
end

local function remove_tree(path)
  if not fs.exists(path) then return true end
  if fs.isDirectory(path) then
    for name in fs.list(path) do
      if name ~= "." and name ~= ".." then
        local child = path .. "/" .. name
        remove_tree(child)
      end
    end
  end
  if path ~= "." then
    pcall(fs.remove, path)
  end
  return true
end

local function clean_worktree(root)
  if not fs.exists(root) then return true end
  for name in fs.list(root) do
    if name ~= ".git" and name ~= "." and name ~= ".." then
      remove_tree(root .. "/" .. name)
    end
  end
end

local function http_get(url)
  local req = internet.request(url)
  if not req then
    return nil, nil, nil, "request failed"
  end

  local chunks = {}
  for chunk in req do
    chunks[#chunks + 1] = chunk
  end
  local body = table.concat(chunks)

  local mt = getmetatable(req)
  local code, message, headers = nil, nil, nil
  if mt and mt.__index and mt.__index.response then
    code, message, headers = mt.__index.response()
  end
  return body, code, message, headers
end

local function is_smart_http_response(body, headers)
  if not body then return false end
  if body:sub(1, 8) == "001e# se" then
    return true
  end
  if body:find("service=git%-upload%-pack", 1, true) then
    return true
  end
  if headers then
    local ct = headers["Content-Type"] or headers["content-type"]
    if ct and tostring(ct):find("application/x%-git%-upload%-pack") then
      return true
    end
  end
  return false
end

local function parse_refs_list(body)
  local refs = {}
  for line in body:gmatch("[^\r\n]+") do
    local sha, ref = line:match("^([0-9a-fA-F]+)%s+(.+)$")
    if sha and ref then
      refs[ref] = sha:lower()
    end
  end
  return refs
end

local function fetch_remote_head(remote_url)
  local body, code, _, _ = http_get(join_url(remote_url, "HEAD"))
  if code ~= 200 or not body then
    return nil
  end
  local ref = body:match("^ref:%s*(%S+)")
  return ref
end

local function fetch_remote_refs(remote_url)
  local body, code, _, headers = http_get(join_url(remote_url, "info/refs"))
  if not body or (code and code ~= 200 and code ~= 304) then
    return nil, "failed to fetch info/refs"
  end

  if is_smart_http_response(body, headers) then
    return nil, "remote uses smart HTTP or pkt-line refs; this script only supports dumb HTTP"
  end

  local refs = parse_refs_list(body)
  return refs
end

local function choose_branch_ref(remote_url, refs)
  local head_ref = fetch_remote_head(remote_url)
  if head_ref and refs[head_ref] then
    return head_ref, refs[head_ref]
  end

  if refs["refs/heads/main"] then
    return "refs/heads/main", refs["refs/heads/main"]
  end

  if refs["refs/heads/master"] then
    return "refs/heads/master", refs["refs/heads/master"]
  end

  for ref, sha in pairs(refs) do
    if ref:match("^refs/heads/") then
      return ref, sha
    end
  end

  return nil, nil
end

local function object_path(repo_dir, sha)
  return repo_dir .. "/.git/objects/" .. sha:sub(1, 2) .. "/" .. sha:sub(3)
end

local function loose_object_exists(repo_dir, sha)
  return fs.exists(object_path(repo_dir, sha))
end

local function fetch_loose_object(repo_dir, remote_url, sha)
  local path = object_path(repo_dir, sha)
  if fs.exists(path) then
    return true
  end

  local url = join_url(remote_url, "objects/" .. sha:sub(1, 2) .. "/" .. sha:sub(3))
  local body, code = http_get(url)
  if not body or code ~= 200 then
    return nil, "missing loose object " .. sha
  end

  local parent = path:match("^(.*)/[^/]+$")
  if parent and not fs.exists(parent) then
    local ok, err = mkdir_p(parent)
    if not ok then
      return nil, err
    end
  end

  local ok, err = write_file(path, body)
  if not ok then
    return nil, err
  end
  return true
end

local function load_loose_object(repo_dir, sha)
  local path = object_path(repo_dir, sha)
  local raw = read_file(path)
  if not raw then
    return nil, "object not present locally: " .. sha
  end

  local ok, inflated = pcall(data.inflate, raw)
  if not ok then
    return nil, "failed to inflate object " .. sha .. ": " .. tostring(inflated)
  end

  local nul = inflated:find("\0", 1, true)
  if not nul then
    return nil, "invalid loose object: " .. sha
  end

  local header = inflated:sub(1, nul - 1)
  local obj_type, size = header:match("^(%S+)%s+(%d+)$")
  if not obj_type then
    return nil, "invalid loose object header: " .. sha
  end

  local content = inflated:sub(nul + 1)
  if tonumber(size) ~= #content then
    return nil, "size mismatch for object " .. sha
  end

  return {
    type = obj_type,
    size = tonumber(size),
    content = content
  }
end

local function ensure_object(repo_dir, remote_url, sha, seen)
  seen = seen or {}
  sha = sha:lower()

  if seen[sha] then
    return true
  end
  seen[sha] = true

  local ok, err = fetch_loose_object(repo_dir, remote_url, sha)
  if not ok then
    return nil, err
  end

  local obj, load_err = load_loose_object(repo_dir, sha)
  if not obj then
    return nil, load_err
  end

  if obj.type == "commit" then
    local tree = obj.content:match("\ntree ([0-9a-f]+)")
    if tree then
      local ok2, err2 = ensure_object(repo_dir, remote_url, tree, seen)
      if not ok2 then return nil, err2 end
    end

    for parent in obj.content:gmatch("\nparent ([0-9a-f]+)") do
      local ok2, err2 = ensure_object(repo_dir, remote_url, parent, seen)
      if not ok2 then return nil, err2 end
    end

  elseif obj.type == "tree" then
    local i = 1
    while i <= #obj.content do
      local space = obj.content:find(" ", i, true)
      if not space then break end
      local nul = obj.content:find("\0", space + 1, true)
      if not nul then break end

      local mode = obj.content:sub(i, space - 1)
      local sha_bin = obj.content:sub(nul + 1, nul + 20)
      local entry_sha = bin_to_hex(sha_bin)

      local ok2, err2 = ensure_object(repo_dir, remote_url, entry_sha, seen)
      if not ok2 then return nil, err2 end

      i = nul + 21
    end
  end

  return true
end

local function parse_tree_entries(tree_content)
  local entries = {}
  local i = 1

  while i <= #tree_content do
    local space = tree_content:find(" ", i, true)
    if not space then break end

    local nul = tree_content:find("\0", space + 1, true)
    if not nul then break end

    local mode = tree_content:sub(i, space - 1)
    local name = tree_content:sub(space + 1, nul - 1)
    local sha_bin = tree_content:sub(nul + 1, nul + 20)
    local sha = bin_to_hex(sha_bin)

    entries[#entries + 1] = {
      mode = mode,
      name = name,
      sha = sha
    }

    i = nul + 21
  end

  return entries
end

local function checkout_tree(repo_dir, remote_url, tree_sha, target_dir)
  local tree_obj, err = load_loose_object(repo_dir, tree_sha)
  if not tree_obj then
    return nil, err
  end
  if tree_obj.type ~= "tree" then
    return nil, "expected tree object, got " .. tree_obj.type
  end

  mkdir_p(target_dir)

  for _, entry in ipairs(parse_tree_entries(tree_obj.content)) do
    local out_path = target_dir .. "/" .. entry.name

    if entry.mode == "040000" or entry.mode == "40000" or entry.mode == "400000" then
      local ok, err2 = checkout_tree(repo_dir, remote_url, entry.sha, out_path)
      if not ok then return nil, err2 end

    elseif entry.mode == "160000" then
      io.stderr:write("Skipping submodule entry: " .. out_path .. "\n")

    else
      local blob_obj, blob_err = load_loose_object(repo_dir, entry.sha)
      if not blob_obj then
        return nil, blob_err
      end
      if blob_obj.type ~= "blob" then
        return nil, "expected blob for " .. out_path .. ", got " .. blob_obj.type
      end

      local parent = out_path:match("^(.*)/[^/]+$")
      if parent and parent ~= "" then
        mkdir_p(parent)
      end

      local f, ferr = io.open(out_path, "wb")
      if not f then
        return nil, ferr
      end
      f:write(blob_obj.content)
      f:close()
    end
  end

  return true
end

local function checkout_commit(repo_dir, remote_url, commit_sha, target_dir)
  local commit_obj, err = load_loose_object(repo_dir, commit_sha)
  if not commit_obj then
    return nil, err
  end
  if commit_obj.type ~= "commit" then
    return nil, "expected commit object, got " .. commit_obj.type
  end

  local tree_sha = commit_obj.content:match("\ntree ([0-9a-f]+)")
  if not tree_sha then
    return nil, "commit has no tree: " .. commit_sha
  end

  clean_worktree(target_dir)
  local ok, err2 = checkout_tree(repo_dir, remote_url, tree_sha, target_dir)
  if not ok then return nil, err2 end

  return true, tree_sha
end

local function ensure_repo_layout(repo_dir)
  mkdir_p(repo_dir)
  mkdir_p(repo_dir .. "/.git")
  mkdir_p(repo_dir .. "/.git/objects")
  mkdir_p(repo_dir .. "/.git/refs")
  mkdir_p(repo_dir .. "/.git/refs/heads")
end

local function save_remote_meta(repo_dir, remote_url, branch_ref)
  write_file(repo_dir .. "/.git/gitlite.remote", "url=" .. remote_url .. "\nref=" .. branch_ref .. "\n")
end

local function load_remote_meta(repo_dir)
  local text = read_file(repo_dir .. "/.git/gitlite.remote")
  if not text then return nil end
  local meta = {}
  for line in text:gmatch("[^\r\n]+") do
    local k, v = line:match("^(%w+)=(.*)$")
    if k then meta[k] = v end
  end
  if not meta.url or not meta.ref then
    return nil
  end
  return meta
end

local function write_head(repo_dir, branch_ref, sha)
  write_file(repo_dir .. "/.git/HEAD", "ref: " .. branch_ref .. "\n")
  write_file(repo_dir .. "/.git/" .. branch_ref, sha .. "\n")
end

local function read_head(repo_dir)
  local head = read_file(repo_dir .. "/.git/HEAD")
  if not head then return nil end
  local ref = head:match("^ref:%s*(%S+)")
  if not ref then
    return nil
  end
  local sha = read_file(repo_dir .. "/.git/" .. ref)
  if sha then
    sha = trim(sha)
  end
  return ref, sha
end

local function clone_repo(remote_url, repo_dir)
  repo_dir = repo_dir or (remote_url:match("([^/]+)%.git/?$") or remote_url:match("([^/]+)/?$") or "repo")
  remote_url = trim_slashes(remote_url)

  if fs.exists(repo_dir) then
    return nil, "target already exists: " .. repo_dir
  end

  ensure_repo_layout(repo_dir)

  local refs, err = fetch_remote_refs(remote_url)
  if not refs then
    return nil, err
  end

  local branch_ref, head_sha = choose_branch_ref(remote_url, refs)
  if not branch_ref or not head_sha then
    return nil, "could not determine default branch from remote"
  end

  io.stdout:write("Selected " .. branch_ref .. " @ " .. head_sha .. "\n")
  save_remote_meta(repo_dir, remote_url, branch_ref)
  write_head(repo_dir, branch_ref, head_sha)

  local ok, fetch_err = ensure_object(repo_dir, remote_url, head_sha, {})
  if not ok then
    return nil, fetch_err
  end

  local ok2, checkout_err = checkout_commit(repo_dir, remote_url, head_sha, repo_dir)
  if not ok2 then
    return nil, checkout_err
  end

  return true
end

local function pull_repo(repo_dir)
  local meta = load_remote_meta(repo_dir)
  if not meta then
    return nil, "missing .git/gitlite.remote in " .. repo_dir
  end

  local branch_ref, local_sha = read_head(repo_dir)
  if not branch_ref then
    branch_ref = meta.ref
  end

  local refs, err = fetch_remote_refs(meta.url)
  if not refs then
    return nil, err
  end

  local remote_sha = refs[branch_ref]
  if not remote_sha then
    return nil, "remote no longer advertises " .. branch_ref
  end

  if local_sha and local_sha:lower() == remote_sha:lower() then
    print("Already up to date.")
    return true
  end

  print("Updating " .. branch_ref .. " -> " .. remote_sha)
  local ok, fetch_err = ensure_object(repo_dir, meta.url, remote_sha, {})
  if not ok then
    return nil, fetch_err
  end

  local ok2, checkout_err = checkout_commit(repo_dir, meta.url, remote_sha, repo_dir)
  if not ok2 then
    return nil, checkout_err
  end

  write_head(repo_dir, branch_ref, remote_sha)
  save_remote_meta(repo_dir, meta.url, branch_ref)

  return true
end

local function sync_repo(remote_url, repo_dir)
  if repo_dir and fs.exists(repo_dir .. "/.git/gitlite.remote") then
    return pull_repo(repo_dir)
  end
  return clone_repo(remote_url, repo_dir)
end

local function usage()
  print([[
gitlite - minimal dumb-HTTP Git client for OpenComputers

Usage:
  gitlite clone <url> [directory]
  gitlite pull <directory>
  gitlite sync <url> [directory]

Examples:
  gitlite clone https://example.com/project.git
  gitlite clone https://example.com/project.git myproject
  gitlite pull myproject
]])
end

local args = {...}
local cmd = args[1]

if not cmd then
  usage()
  return
end

local ok, err
if cmd == "clone" then
  ok, err = clone_repo(args[2], args[3])
elseif cmd == "pull" then
  ok, err = pull_repo(args[2])
elseif cmd == "sync" then
  ok, err = sync_repo(args[2], args[3])
else
  usage()
  return
end

if not ok then
  io.stderr:write(tostring(err) .. "\n")
  os.exit(1)
end