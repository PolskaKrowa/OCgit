-- OCgit.lua  –  A git-like CLI for OpenOS, backed by clone.lua
--
-- Usage:
--   OCgit clone <remote-url> [directory]
--   OCgit help [command]

local term      = require("term")
local component = require("component")
local gpu       = component.gpu

-------------------------------------------------------------------------------
-- Colour helpers (gracefully degrade on monochrome displays)
-------------------------------------------------------------------------------
local HAS_COLOR = gpu.maxDepth() > 1

local colors = {
  reset   = 0xFFFFFF,
  dim     = 0x888888,
  green   = 0x00CC66,
  yellow  = 0xFFCC00,
  red     = 0xFF4444,
  cyan    = 0x44DDFF,
  white   = 0xFFFFFF,
}

local function set_fg(color)
  if HAS_COLOR then gpu.setForeground(color) end
end

local function reset_color()
  set_fg(colors.white)
end

-- Print helpers that restore color afterwards
local function print_color(color, text)
  set_fg(color)
  io.write(text)
  reset_color()
end

local function println_color(color, text)
  print_color(color, text .. "\n")
end

-------------------------------------------------------------------------------
-- Banner / branding
-------------------------------------------------------------------------------
local function print_banner()
  set_fg(colors.cyan)
  io.write("  ╔═╗╔═╗┌─┐┬┌┬┐\n")
  io.write("  ║ ║║  │ ┬│ │ \n")
  io.write("  ╚═╝╚═╝└─┘┴ ┴ \n")
  reset_color()
  set_fg(colors.dim)
  io.write("  OpenComputers Git  -  Git for OpenComputers!\n\n")
  reset_color()
end

-------------------------------------------------------------------------------
-- Usage / help
-------------------------------------------------------------------------------
local COMMANDS = {}  -- populated below

local function print_usage()
  print_banner()
  println_color(colors.yellow, "Usage:")
  io.write("  OCgit <command> [options]\n\n")
  println_color(colors.yellow, "Commands:")
  for name, cmd in pairs(COMMANDS) do
    set_fg(colors.green)
    io.write(string.format("  %-12s", name))
    reset_color()
    io.write(cmd.short_desc .. "\n")
  end
  io.write("\n")
  set_fg(colors.dim)
  io.write("Run 'OCgit help <command>' for details on a specific command.\n")
  reset_color()
end

local function print_command_help(name)
  local cmd = COMMANDS[name]
  if not cmd then
    println_color(colors.red, "Unknown command: " .. tostring(name))
    print_usage()
    return
  end

  println_color(colors.cyan, "OCgit " .. name)
  io.write(cmd.long_desc .. "\n\n")
  if cmd.usage then
    println_color(colors.yellow, "Usage:")
    io.write("  " .. cmd.usage .. "\n\n")
  end
  if cmd.examples then
    println_color(colors.yellow, "Examples:")
    for _, ex in ipairs(cmd.examples) do
      set_fg(colors.dim)
      io.write("  " .. ex .. "\n")
    end
    reset_color()
  end
end

-------------------------------------------------------------------------------
-- Error / success formatting
-------------------------------------------------------------------------------
local function die(msg)
  set_fg(colors.red)
  io.write("error: " .. msg .. "\n")
  reset_color()
  os.exit(1)
end

local function info(msg)
  set_fg(colors.dim)
  io.write("  " .. msg .. "\n")
  reset_color()
end

local function success(msg)
  println_color(colors.green, "✓ " .. msg)
end

-------------------------------------------------------------------------------
-- Command: help
-------------------------------------------------------------------------------
COMMANDS["help"] = {
  short_desc = "Show help for a command",
  long_desc  = "Display general usage or detailed help for a specific command.",
  usage      = "OCgit help [command]",
  examples   = {
    "OCgit help",
    "OCgit help clone",
  },
  run = function(args)
    if args[1] then
      print_command_help(args[1])
    else
      print_usage()
    end
  end,
}

-------------------------------------------------------------------------------
-- Command: clone
-------------------------------------------------------------------------------
COMMANDS["clone"] = {
  short_desc = "Clone a remote repository",
  long_desc  = [[Clone a Git repository over HTTPS using the Smart HTTP protocol
(Git protocol v2). Creates the target directory, populates .git/,
and checks out the default branch (main or master).

Requires: internet card, data card (tier 2+)]],
  usage    = "OCgit clone <url> [directory]",
  examples = {
    "OCgit clone https://github.com/user/repo",
    "OCgit clone https://github.com/user/repo  myproject",
  },
  run = function(args)
    local url    = args[1]
    local target = args[2]

    if not url then
      print_command_help("clone")
      die("a URL is required")
    end

    -- Derive a sensible target directory from the URL when none given
    if not target then
      target = url:match("/([^/]+)$") or "repo"
      target = target:gsub("%.git$", "")  -- strip trailing .git
    end

    -- Sanity-check for an internet card
    if not component.isAvailable("internet") then
      die("no internet card detected – please install one")
    end

    print_banner()
    println_color(colors.cyan, "Cloning " .. url)
    info("into directory: " .. target)
    io.write("\n")

    -- Load clone; look beside this script first, then on the Lua path
    local ok, clone = pcall(require, "clone")
    if not ok then
      die("could not load clone.lua – make sure it is on your path\n       " .. tostring(clone))
    end

    local clone_ok, err = pcall(clone.clone, url, target)
    if not clone_ok then
      die(tostring(err))
    end

    io.write("\n")
    success("Done!  Repository cloned into '" .. target .. "'")
  end,
}

-------------------------------------------------------------------------------
-- Dispatch
-------------------------------------------------------------------------------
local raw_args = { ... }   -- OpenOS passes CLI args via vararg

-- Shift off the command name
local cmd_name = table.remove(raw_args, 1)

if not cmd_name or cmd_name == "" then
  print_usage()
  os.exit(0)
end

local cmd = COMMANDS[cmd_name]
if not cmd then
  println_color(colors.red, "OCgit: '" .. cmd_name .. "' is not a known command.\n")
  print_usage()
  os.exit(1)
end

cmd.run(raw_args)