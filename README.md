# OCgit

> **Git for OpenComputers** — clone real GitHub repositories directly from inside Minecraft.

OCgit is a pure-Lua Git client for [OpenComputers](https://github.com/MightyPirates/OpenComputers) running on OpenOS. It speaks the Git Smart HTTP protocol (v2 with v1 fallback), fetches packfiles, resolves deltas, and checks out a working tree — all without leaving your Minecraft world.

---

## Features

- **`OCgit clone`** — clones any public HTTPS Git repository
- Git Smart HTTP protocol v2 (with automatic v1 fallback)
- Packfile parsing with OFS and REF delta resolution
- Pure-Lua zlib inflate (via `deflate.lua`) with automatic fallback to the OpenComputers data card
- Coloured terminal output with graceful monochrome degradation
- Verbose debug logging (toggleable per-module)

---

## Requirements

| Component | Minimum |
|---|---|
| Internet Card | Any tier |
| Data Card | Tier 2+ *(only needed if `deflate.lua` fails to load)* |
| RAM | 512 KB recommended |

### Third-party dependencies

These are fetched automatically by the installer — you don't need to grab them manually.

| Library | Source | Purpose |
|---|---|---|
| `deflate.lua` | [Magik6k-Programs/libdeflate](https://github.com/OpenPrograms/Magik6k-Programs/tree/master/libdeflate) | Pure-Lua zlib inflate |
| `crc32.lua` | [Magik6k-Programs/libcrc32](https://github.com/OpenPrograms/Magik6k-Programs/tree/master/libcrc32) | CRC-32 (required by deflate) |

---

## Installation

Paste this into your OpenOS terminal to download and run the installer:

```sh
wget https://raw.githubusercontent.com/PolskaKrowa/OCgit/refs/heads/main/install.lua install.lua && install
```

The installer will:
1. Download `OCgit.lua` → `/bin/`
2. Download all library modules → `/lib/`
3. Offer to reboot your computer

---

## Usage

```sh
OCgit clone <url> [directory]
OCgit help [command]
```

### Examples

```sh
# Clone a repository (directory name inferred from URL)
OCgit clone https://github.com/user/repo

# Clone into a specific directory
OCgit clone https://github.com/user/repo  myproject

# Get help on a specific command
OCgit help clone
```

---

## Project Structure

```
OCgit/
├── install.lua          # One-shot installer
└── src/
    ├── OCgit.lua        # CLI entry point  →  /bin/OCgit.lua
    └── lib/
        ├── clone.lua        # Top-level clone orchestration
        ├── protocol.lua     # Git Smart HTTP (discover refs, fetch pack)
        ├── packfile.lua     # Packfile parser & delta application
        ├── pack_inflate.lua # zlib inflate (cpu + data card paths)
        ├── checkout.lua     # Tree walker & file writer
        ├── util.lua         # SHA-1, pkt-line, HTTP helpers
        └── debug.lua        # Debug flags & logging helpers
```

The installer also fetches these third-party libraries into `/lib/` automatically:

```
/lib/deflate.lua   ← github.com/OpenPrograms/Magik6k-Programs  (libdeflate)
/lib/crc32.lua     ← github.com/OpenPrograms/Magik6k-Programs  (libcrc32)
```

---

## Debug Logging

Debug output is controlled by three flags in `debug.lua`:

```lua
M.DEBUG         = true   -- master switch (general trace)
M.DEBUG_INFLATE = true   -- per-object zlib inflate tracing (noisy)
M.DEBUG_DELTA   = true   -- per-instruction delta tracing (very noisy)
```

Set any of these to `false` to silence that layer. For normal use it's recommended to set all three to `false` after installation.

---

## How It Works

1. **Discover refs** — sends a `ls-refs` command over Smart HTTP to enumerate the remote's branches
2. **Fetch packfile** — sends a `fetch` command requesting the HEAD commit; the server streams a packfile back over sideband channel 1
3. **Parse packfile** — unpacks every object (commits, trees, blobs) and resolves OFS/REF deltas in-order
4. **Write loose objects** — each object is zlib-compressed and written to `.git/objects/xx/yyyy…`
5. **Checkout** — walks the root tree recursively, writing blob contents to the working directory

---

## Limitations

- **Read-only** — only `clone` is implemented. There is no `commit`, `push`, or `pull`.
- **Public repos only** — no authentication support.
- **HTTPS only** — SSH is not supported by the OpenComputers internet card.
- **Single branch** — checks out `main` or `master` (whichever exists); other branches are not yet selectable.

---

## Contributing

Issues and pull requests are welcome! If you run into a repository that fails to clone, opening an issue with the debug log output (all three debug flags enabled) is the fastest way to get it fixed.

---

## License

Licensed under the **Apache License, Version 2.0**. See [LICENSE](LICENSE) for the full text.

```
Copyright 2024 PolskaKrowa

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```