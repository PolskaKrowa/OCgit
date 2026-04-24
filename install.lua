-- install.lua  –  OCgit installer for OpenOS
--
-- Downloads each source file individually from GitHub and places them into
-- the correct locations:
--   src/OCgit.lua      →  /bin/OCgit.lua
--   src/lib/*.lua      →  /lib/*.lua
--
-- Also fetches third-party dependencies from OpenPrograms/Magik6k-Programs:
--   deflate.lua        →  /lib/deflate.lua   (pure-Lua zlib inflate)
--   crc32.lua          →  /lib/crc32.lua     (CRC-32 used by deflate)
--
-- Then optionally reboots the computer.

local component = require("component")
local filesystem = require("filesystem")
local internet   = require("internet")
local term       = require("term")

-------------------------------------------------------------------------------
-- Colour helpers
-------------------------------------------------------------------------------
local gpu = component.gpu
local HAS_COLOR = gpu.maxDepth() > 1

local C = {
  reset  = 0xFFFFFF,
  dim    = 0x888888,
  green  = 0x00CC66,
  yellow = 0xFFCC00,
  red    = 0xFF4444,
  cyan   = 0x44DDFF,
}

local function fg(color)
  if HAS_COLOR then gpu.setForeground(color) end
end

local function reset() fg(C.reset) end

local function cprint(color, text)
  fg(color); io.write(text); reset()
end

local function cprintln(color, text)
  cprint(color, text .. "\n")
end

-------------------------------------------------------------------------------
-- Banner
-------------------------------------------------------------------------------
local function banner()
  fg(C.cyan)
  io.write("  ╔═╗╔═╗┌─┐┬┌┬┐  Installer\n")
  io.write("  ║ ║║  │ ┬│ │ \n")
  io.write("  ╚═╝╚═╝└─┘┴ ┴ \n")
  reset()
  fg(C.dim)
  io.write("  OpenComputers Git  -  github.com/PolskaKrowa/OCgit\n\n")
  reset()
end

-------------------------------------------------------------------------------
-- Files to install.
-- Each entry is { url = <full raw URL>, dest = <absolute local path> }.
--
-- OCgit source files are fetched from this repo's main branch.
-- Third-party dependencies are fetched from OpenPrograms/Magik6k-Programs.
-------------------------------------------------------------------------------
local OCGIT_BASE = "https://raw.githubusercontent.com/PolskaKrowa/OCgit/refs/heads/main/"
local MAGIK_BASE = "https://raw.githubusercontent.com/OpenPrograms/Magik6k-Programs/refs/heads/master/"

local FILES = {
  -- OCgit files
  { url = OCGIT_BASE .. "src/OCgit.lua",            dest = "/bin/OCgit.lua"          },
  { url = OCGIT_BASE .. "src/lib/clone.lua",         dest = "/lib/clone.lua"          },
  { url = OCGIT_BASE .. "src/lib/checkout.lua",      dest = "/lib/checkout.lua"       },
  { url = OCGIT_BASE .. "src/lib/packfile.lua",      dest = "/lib/packfile.lua"       },
  { url = OCGIT_BASE .. "src/lib/pack_inflate.lua",  dest = "/lib/pack_inflate.lua"   },
  { url = OCGIT_BASE .. "src/lib/protocol.lua",      dest = "/lib/protocol.lua"       },
  { url = OCGIT_BASE .. "src/lib/util.lua",          dest = "/lib/util.lua"           },
  { url = OCGIT_BASE .. "src/lib/debug.lua",         dest = "/lib/debug.lua"          },
  -- Third-party dependencies (OpenPrograms / Magik6k)
  { url = MAGIK_BASE .. "libdeflate/deflate.lua",    dest = "/lib/deflate.lua"        },
  { url = MAGIK_BASE .. "libcrc32/crc32.lua",        dest = "/lib/crc32.lua"          },
}

-------------------------------------------------------------------------------
-- Helpers
-------------------------------------------------------------------------------
local function die(msg)
  cprintln(C.red, "\nerror: " .. msg)
  os.exit(1)
end

local function ensure_dir(path)
  if not filesystem.isDirectory(path) then
    local ok, err = filesystem.makeDirectory(path)
    if not ok then
      die("could not create directory " .. path .. ": " .. tostring(err))
    end
  end
end

-- Download url → string, or die on failure.
local function fetch(url)
  local ok, result = pcall(function()
    local req    = internet.request(url)
    local chunks = {}
    for chunk in req do
      chunks[#chunks + 1] = chunk
    end
    return table.concat(chunks)
  end)

  if not ok or not result or #result == 0 then
    die("download failed for " .. url .. "\n       " .. tostring(result))
  end
  return result
end

-- Write string to absolute path, creating parent dirs as needed.
local function write_file(path, data)
  local dir = filesystem.path(path)
  ensure_dir(dir)

  local f, err = io.open(path, "wb")
  if not f then
    die("could not open " .. path .. " for writing: " .. tostring(err))
  end
  f:write(data)
  f:close()
end

-- Ask a yes/no question; return true for y/Y.
local function ask(question)
  cprint(C.yellow, question .. " [y/N] ")
  local answer = io.read()
  return answer and answer:lower():sub(1, 1) == "y"
end

-------------------------------------------------------------------------------
-- Main
-------------------------------------------------------------------------------
banner()

-- Sanity checks
if not component.isAvailable("internet") then
  die("no internet card detected – please install one")
end

cprintln(C.yellow, "Installing " .. #FILES .. " file(s)…\n")

ensure_dir("/bin")
ensure_dir("/lib")

for i, entry in ipairs(FILES) do
  cprint(C.dim,    string.format("  [%d/%d] ", i, #FILES))
  cprint(C.cyan,   entry.dest)
  io.write("  ")

  local data = fetch(entry.url)
  write_file(entry.dest, data)

  cprintln(C.green, "✓")
  os.sleep(0)   -- yield so the display updates
end

io.write("\n")
cprintln(C.green, "Installation complete!")
cprint(C.dim, "  Run ")
cprint(C.cyan, "OCgit help")
cprintln(C.dim, " to get started.\n")

-- Optional reboot
if ask("Reboot now?") then
  cprintln(C.yellow, "Rebooting…")
  os.sleep(0.5)
  require("computer").shutdown(true)   -- true = reboot
else
  cprintln(C.dim, "Reboot skipped – changes take effect after your next restart.")
end