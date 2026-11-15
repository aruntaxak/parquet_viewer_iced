# Parquet Viewer

Native Rust parquet viewer built on `iced` 0.14 and `wgpu`. It opens files with
millions of rows by reading the footer only and paging the data 1500 rows at a time.

## Features

- **Footer-only open** — columns and row count come from file metadata via
  `SerializedFileReader`; no data page is read until a page is loaded.
- **Tail default** — a file opens on its *last* page scrolled to the bottom, so
  `Prev` works immediately and `Next` is disabled at the end. The toolbar button
  reads `Show all` in that state and `Tail 1500` once you are showing all pages.
- **Pagination** — `PAGE_SIZE = 1500` rows per page, `Prev` / `Next`, with
  `Page n/m` in the toolbar. 4,833,919 rows = 3,223 pages.
- **Virtualized grid** — only the rows inside the viewport plus `GRID_OVERSCAN` (8)
  rows on each side are built into widgets; the rest of the page is represented by
  two fixed-height spacers, so the scrollbar keeps the full page length.
- **Resizable columns** — columns split the window width equally; drag the divider
  at the right edge of a header cell to move the boundary between it and the next
  column. Widths rescale proportionally when the window resizes.
- **Sticky header** — the header lives in its own horizontal scroller whose offset
  is mirrored from the body, so columns stay aligned while scrolling sideways.
- **Layout inspector** — footer only: size, version, rows, row groups, columns,
  created-by, key-value metadata, schema (physical and logical type per column), a
  row-group byte strip, and per-chunk codec, sizes, offsets, min/max, null count,
  encodings and flags (bloom / column index / offset index / dictionary page /
  index page).
- **Go to Date** — day, month and year dropdowns plus `Go`. It samples 5 rows to
  detect `dd-mm-yyyy`, `dd/mm/yyyy` or `yyyy-mm-dd`, builds the permutations of the
  chosen date, scans the date column (the first column whose name contains
  `date`) across every row group, then loads from one row before the match so the
  matched date shows as the second row.
- **One-line toolbar**, light theme, grey scrollbars and dropdowns, and SF Mono
  app-wide.
- **Open** by CLI argument, `Open` button, drag & drop, or macOS Open With.

## Install

### Build from source

Requires Rust with edition 2024 support; `iced 0.14` needs Rust 1.88+ (built here
with 1.100.0).

```bash
cargo check
cargo build --release          # -> target/release/parquet-viewer, 23,009,168 bytes
./target/release/parquet-viewer
```

### macOS app bundle

```bash
./scripts/bundle_macos.sh                    # installs /Applications/Parquet Viewer.app
./scripts/bundle_macos.sh /path/to/Other.app # or a custom location
```

The script builds the release binary, assembles the bundle with `Info.plist` and
the icon, compiles the `Parquet Opener` helper droplet with `osacompile`, patches
the droplet's plist, ad-hoc signs both, and registers both with `lsregister`.

## Usage

```bash
./target/release/parquet-viewer                            # opens the file dialog
./target/release/parquet-viewer ~/data/BTCUSDT-1m.parquet  # CLI argument
cargo run --release -- ~/data.parquet
```

Arguments beginning with `-` are ignored; the first argument that resolves to an
existing file is opened. `file://` URLs and `~/` are resolved, and the path is
canonicalized.

Toolbar, one row:

```
Open | Show all / Tail 1500 | Data | Layout | Prev | Page n/m | Next | Go to Date | day | month | year | Go | status
```

- `Data` / `Layout` switch the body; the active tab is drawn darker.
- The year list runs from 2010 to the current year.
- Dropdowns clamp the day to the length of the selected month.

Grid alignment: `Date`, `Time` and `Datetime` columns are left-aligned, every other
column is centered.

## Memory

Measured on `~/Documents/Crypto/BTCUSDT-1m.parquet` — 4,833,919 rows × 6 columns,
91 MB, 3,223 pages — with the tail page loaded:

| Metric | Value |
|---|---|
| `phys_footprint` (Activity Monitor "Memory") | 39 MB |
| `ps -o rss` | 104–107 MB |
| `phys_footprint_peak` while loading the page | 132 MB |

RSS is larger than the footprint because most of it is clean framework pages
(`wgpu` / Metal) shared with other processes; `phys_footprint` is the number that
matters.

Launching the app with a file argument avoids showing a native `NSOpenPanel`. If
the dialog is ever used (`Open` button, or no argument), macOS spawns the AppKit
`Open and Save Panel Service` (`com.apple.appkit.xpc.openAndSavePanelService`,
24 MB footprint) and it stays resident afterwards. Check Activity Monitor's
`phys_footprint`, or use `footprint -p <pid>`, when you need the true total.

## macOS Open With

`.parquet` files are associated with **Parquet Opener**
(`Parquet Viewer.app/Contents/Helpers/Parquet Opener.app`), an AppleScript droplet
that forwards dropped files to the viewer as argv via `bundle/launch.sh`. Point the
file type at it once:

1. Right-click a `.parquet` → Get Info → Open With → **Parquet Opener** → Change All.

Why a helper is needed: iced runs on winit, and on a cold launch the `odoc` Apple
Event arrives before the app can register its own handler, so it goes to winit's
`NSApplication` delegate, which drops it. The droplet receives `odoc` natively and
hands the path over as a command-line argument instead.

Warm opens (while the app is running) are received by an `NSAppleEventManager`
handler registered at startup for `aevt/odoc` and `aevt/GURL`; the path is queued
and picked up by an event-driven subscription. The app must not replace
`NSApplication`'s delegate — winit 0.30 aborts if that happens.

`PARQUET_DEBUG=1` appends open-path diagnostics to `/tmp/parquet-viewer-debug.log`.

## Project layout

```
Cargo.toml                # iced 0.14 (smol), parquet 53, arrow 53, rfd 0.16
Cargo.lock
src/main.rs               # single-file app (1,880 lines)
bundle/Info.plist         # main app bundle plist
bundle/opener.applescript # helper droplet source
bundle/launch.sh          # helper -> viewer argv forwarding
bundle/parquet-viewer.icns
scripts/bundle_macos.sh   # assemble + sign + register the .app
target/release/parquet-viewer
```

## Development

See [AGENTS.md](AGENTS.md) for the stack, state model, UI composition and pitfalls.
