# AGENTS.md — Parquet Viewer

Guidelines for AI agents and human contributors working in this repo.

## Overview

Native Rust parquet viewer for large files (millions of rows), single binary
(`23,009,168` bytes) on `iced 0.14` + `wgpu`. The whole application lives in
`src/main.rs`; there is no library target and no other module file.

**Key invariants:**

- `PAGE_SIZE = 1500`, `TAIL_SIZE = 1500` (`src/main.rs:193-195`). Never load the
  whole file. 1500 covers a full 24h trading day (1440 one-minute bars) plus
  context.
- Columnar pushdown: `ParquetRecordBatchReaderBuilder::with_projection(mask)`
  `.with_offset(offset).with_limit(length).with_batch_size(512)`.
- Default page is the last page, scrolled to the bottom (`Tail`); `Tail 1500` /
  `Show all` toggles which end a file opens from.
- `Go to Date` uses day / month / year `pick_list`s plus `Go`.
- Only the viewport rows are rendered; the rest of the page is spacer height.

## Stack & versions

```toml
iced = { version = "0.14", features = ["smol"] }
parquet = { version = "53", features = ["arrow"] }
arrow = "53"
rfd = "0.16"
chrono = "0.4"
regex = "1.10"
dirs = "5.0"
url = "2.5"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# macOS only
objc2 = "0.5.2"
objc2-app-kit = { version = "0.2", features = ["NSResponder", "NSApplication", "NSMenu", "NSMenuItem"] }
objc2-foundation = { version = "0.2", features = ["NSString"] }
block2 = "0.5"
```

- Edition 2024. `iced 0.14` needs Rust 1.88+; this repo is built with 1.100.0.
- The `smol` feature is required: the default `thread-pool` backend's `iced::time`
  module is empty, so `iced::time::every` does not exist without it.
- `objc2` / `objc2-app-kit` are used only by the `macos_open` module for the
  AppleEvent handler.
- `cargo build --release` finished in 3.4 s incremental with dependencies cached;
  `cargo check` took 14.8 s on this tree.

## File layout

```
Cargo.toml
Cargo.lock
src/main.rs               # entire app, 1,880 lines
bundle/Info.plist         # main app bundle plist
bundle/opener.applescript # AppleScript droplet for .parquet association
bundle/launch.sh          # droplet -> viewer argv forwarding
bundle/parquet-viewer.icns
scripts/bundle_macos.sh   # assemble, sign, register /Applications/Parquet Viewer.app
target/release/parquet-viewer
```

## Source conventions (`src/main.rs`)

- `resolve_path()` (`:235`) handles `file://` URLs, `~/`, and `canonicalize`.
- `get_total_rows()` / `get_columns()` (`:258`, `:264`) open a
  `SerializedFileReader` and read footer metadata only — no data pages.
- `array_value_to_string()` (`:271`) / `array_to_json_value()` (`:304`) fast paths:
  `Utf8`, `LargeUtf8`, `Int32`, `Int64`, `Float32`, `Float64`, `Boolean`, plus
  `Date32` as a raw int in the JSON path. `Timestamp(_, _)` has an explicit arm
  that routes to `array_fallback_to_string()`.
- `array_fallback_to_string()` (`:294`) uses `ArrayFormatter` with default (ISO)
  options. Every other type routes through it. Never use `format!("{:?}", array)`
  — that renders the whole column into one cell.
- `read_slice_columnar(path, columns, offset, length)` (`:324`) builds a
  `ProjectionMask::roots(parquet_schema, indices)` from the requested column names
  and returns `HashMap<String, Vec<Value>>`. Non-finite floats become strings.
- `get_row_group_info()` (`:356`) returns `(num_row_groups, rows_per_group)` from
  the footer, used by the date scan.
- `get_layout_info()` (`:489`) is footer-only: file size, version, rows, row
  groups, created-by, key-value metadata, schema columns (physical + logical
  type), and per-chunk codec, sizes, offsets, statistics, null count and flags.
  `stat_strings()` (`:444`) and `ba_to_string()` (`:431`) format statistics.
- `cell_text()` (`:676`) turns a `serde_json::Value` into a grid string; `Null`
  becomes the empty string.
- `column_alignment()` (`:685`) left-aligns `date` / `time` / `datetime` and
  centers everything else.
- `debug_log()` (`:226`) appends to `/tmp/parquet-viewer-debug.log` only when
  `PARQUET_DEBUG` is set.

### State (`ParquetViewerApp`, `:611`)

```
path, total_rows, columns, data HashMap<String, Vec<Value>>, rows Vec<Rc<Vec<String>>>,
grid_window Range<usize>, grid_offset_x, col_widths Vec<f32>, resizing Option<ResizeState>,
window_width, window_height, current_page, total_pages, show_all, show_layout,
layout Option<LayoutInfo>, status, scroll Option<ScrollTarget>,
sel_year, sel_month, sel_day, auto_open_done, started, hovering,
schema_open, kv_open, expanded_rgs HashSet<usize>
```

- `rows` is the page's text cache: one `Rc<Vec<String>>` per row, rebuilt by
  `rebuild_rows()` (`:960`) after every page load. All 1500 page rows are built;
  only `grid_window` is turned into widgets.
- `grid_window` is the rendered row range. `grid_capacity()` (`:902`) estimates it
  as `ceil(window_height / 20.0) + 2 * GRID_OVERSCAN` for page loads and resizes;
  `Message::GridScrolled` recomputes the exact range from the scroll offset and
  the viewport height.
- `col_widths` starts empty. `reset_column_widths()` (`:909`) divides
  `window_width - (columns - 1)` separator pixels equally (floor `GRID_MIN_COL_W`);
  a window resize then scales every width by the new/old available ratio.
- `scroll` is `Option<ScrollTarget>` = `Top | Bottom | Row(n)`.
  `take_scroll_task()` (`:1134`) converts it to a widget operation and clears it.
- `Default` (`:641`) uses today's date, `status: "No file loaded - choose a parquet
  file"`, `grid_window: 0..GRID_DEFAULT_ROWS`, `window_width/height: 750/950`.

### Application flow

- `main()` (`:1853`) walks `std::env::args().skip(1)`, skips anything starting with
  `-`, resolves each argument and takes the first that is an existing file, then
  hands it to `boot` through the `iced::application` closure.
- `boot(initial)` (`:874`) opens that path and sets `auto_open_done = true`. It
  never calls `rfd`: there is no window yet.
- `open_path()` (`:927`) → `get_columns` → `reset_column_widths` → `get_total_rows`
  → `update_total_pages` → `get_layout_info` (row groups auto-expanded when there
  are ≤ 4) → `status = "{rows} rows x {cols} cols"` → `load_page(last)` when not
  `show_all`, else `load_page(0)`.
- `load_page(page)` (`:974`) clamps the page, then reads
  `min(PAGE_SIZE, total - page * PAGE_SIZE)` rows starting at `page * PAGE_SIZE`.
  It sets `scroll = Bottom` on the last page when not `show_all`, otherwise
  `Top`, and seeds `grid_window` from `grid_capacity()`.
- `load_at(start)` (`:1009`) loads a non-page-aligned window of up to `PAGE_SIZE`
  rows and sets `scroll = Row(1)` so the matched date is the second row.
- `goto_date()` (`:1034`) samples 5 rows to detect the date format, builds the
  permutations, prioritises the variants that match the detected format, scans row
  groups with a single-column `ProjectionMask` at `batch_size 8192`, comparing
  `==`, `contains`, and the first 10 characters, then calls
  `load_at(real_idx - 1)`.

### UI composition

- `view()` (`:1488`) stacks toolbar + body, and overlays the drag-and-drop target
  when `hovering`.
- `view_toolbar()` (`:1152`) is a **single** row: `Open`, `Show all`/`Tail 1500`,
  `Data`, `Layout`, `Prev`, `Page n/m`, `Next`, `Go to Date`, day, month, year, `Go`,
  then the status text (size 11, clipped, `Length::Fill`). Below it sits a 1px rule
  drawn with the theme's `background.strong` color.
  The active tab uses `tab_active` (darker, 0.80), the inactive one `bar_button`
  (0.90); `Prev`/`Next` lose their `on_press` instead of being styled disabled.
- `view_data()` (`:1267`):
  - Header: `grid_header_row()` (`:747`) — header cells carry a 5px
    `RESIZE_HANDLE_W` hit area stacked at the right edge, wired to
    `Message::ResizeStart(i)`. The header lives in its own horizontal-only
    `scrollable` with `HEADER_ID` and a hidden scrollbar.
  - Body: `scrollable` with `GRID_ID`, `Direction::Both`, styled by
    `grey_scrollable_style()`. The content is
    `[space(first * GRID_PITCH_Y), column(visible rows), space((n - last) * GRID_PITCH_Y)]`
    so the scrollbar reflects the whole page while only `grid_window` is built.
  - Rows are `grid_row()` (`:726`): one `grid_cell()` per column, separated by
    `column_line()` 1px verticals. `grid_cell()` (`:693`) is a fixed
    `GRID_ROW_H`-tall container with 4px horizontal padding.
- `view_layout()` (`:1337`) is a single vertical `scrollable` of sections: file info
  rows, the row-group byte strip (`FillPortion` proportional to
  `total_byte_size`, colored by `palette()`), the schema table, key-value
  metadata, one collapsible entry per row group whose body is a horizontally
  scrollable chunk table, and a legend line.
- `section_button()` (`:1800`) is the collapsible header: `button::text` with a
  `v` / `>` prefix.
- Drag & drop: `window::Event::FileHovered` / `FilesHoveredLeft` / `FileDropped`
  drive `hovering`; the overlay is `rgba(0, 0, 0, 0.35)` with a size-24 label.
- Title (`:1512`): `Parquet Viewer — file — rows × cols`, recomputed from state
  every frame, or `Parquet Viewer` with no file.
- Theme: `Theme::Light`. Window `750x950`, minimum `700x500`.

### Subscriptions (`:1525`)

- `iced::window::events()` → `Message::WindowEvent` (drag-drop, resize, unfocus).
- One-shot 250ms `iced::time::every` → `Message::Started`, only while
  `!started`: registers the macOS AppleEvent handler and consumes the scroll
  target left by the boot page load.
- One-shot `Message::AutoOpen`, only while `!auto_open_done`: 2000ms on macOS,
  250ms elsewhere.
- `macos_open::subscription()` — event-driven stream of `Message::ExternalOpen`.
- While `resizing.is_some()`, a global `iced::event::listen_with` follows the
  pointer for `ResizeMove` / `ResizeEnd`.

### Constants

| Name | Value | Use |
|---|---|---|
| `PAGE_SIZE` / `TAIL_SIZE` | 1500 | rows per page |
| `UI_FONT` / `GRID_FONT` | 12.0 | toolbar and cell text |
| `GRID_ROW_H` / `GRID_PITCH_Y` | 23.6 | cell height and row pitch |
| `ROW_HEIGHT` | 22.0 | `ScrollTarget::Row` offset estimate only |
| `GRID_OVERSCAN` | 8 | rows built outside the viewport |
| `GRID_DEFAULT_ROWS` | 80 | initial `grid_window` before the first scroll |
| `GRID_MIN_COL_W` | 48.0 | column width floor |
| `RESIZE_HANDLE_W` | 5.0 | divider hit area |
| `DATE_FIELD_W` / `YEAR_FIELD_W` | 38.0 / 56.0 | dropdown widths |

### Fonts

`APP_FONT = Font::with_name(".SF NS Mono")` (`:218`) with `BOLD` as the same family
at `Weight::Bold`, passed to `iced::Settings::default_font`. macOS ships only the
variable family `.SF NS Mono` (`/System/Library/Fonts/SFNSMono.ttf`, `wght` axis
294.67–900). cosmic-text maps `Font::weight` onto that axis, so `Weight::Normal`
renders Regular. No font bytes are loaded at runtime.

## Build / run / test

```bash
cargo check                                     # before every release build
cargo build --release                           # -> target/release/parquet-viewer
./target/release/parquet-viewer                 # file dialog
./target/release/parquet-viewer ~/data.parquet  # CLI
cargo run --release -- ~/data.parquet
./scripts/bundle_macos.sh                       # install /Applications/Parquet Viewer.app
```

Test file: `~/Documents/Crypto/BTCUSDT-1m.parquet` — 91 MB, 4,833,919 rows × 6
columns, 3,223 pages.

Manual checks after a UI change: `Prev`/`Next` across page boundaries, `Tail 1500`
↔ `Show all`, `Go` for a date you can see on screen, column drag in both
directions plus a window resize, drag & drop, and the CLI path.

## Memory

Measured on the tail page of the test file:

| Metric | Value |
|---|---|
| `phys_footprint` | 39 MB |
| `ps -o rss` | 104–107 MB |
| `phys_footprint_peak` during page load | 132 MB |

`footprint -p <pid>` is the tool to confirm; RSS overstates the cost because the
`wgpu` / Metal framework pages are clean and shared. Showing the file dialog spawns
Apple's `com.apple.appkit.xpc.openAndSavePanelService` helper (24 MB footprint) as
a separate process, which Activity Monitor lists as a child of the app.

## Common pitfalls

1. **winit 0.30 delegate panic** — replacing `NSApplication`'s delegate aborts with
   `tried to get a delegate that was not the one Winit has registered`. Never add
   `setDelegate`; register only the `NSAppleEventManager` handler.
2. **`iced::time::every` missing** — the thread-pool backend has an empty `time`
   module. Keep `features = ["smol"]`.
3. **`RelativeOffset::END` scrolls horizontally too** — it is `{ x: 1.0, y: 1.0 }`.
   Build `{ x: 0.0, y: 1.0 }` for the tail scroll.
4. **rfd** — the synchronous dialog runs through `Task::perform`; rfd 0.16 uses
   `run_on_main` internally, so that is safe. Never call rfd from `boot` (no window
   yet), and note that opening it spawns an AppKit panel-service process.
5. **`ProjectionMask` privacy** — use `parquet::arrow::ProjectionMask`
   (re-exported), not `parquet::arrow::arrow_reader::ProjectionMask` (private
   field).
6. **`with_row_groups` loads the whole row group** — always use
   `with_offset`/`with_limit` with `batch_size 512` for pagination. The single
   exception is the date scan in `goto_date()`.
7. **Date variants** — generate the permutations, prioritise the detected
   `col_format`, and compare with `==`, `contains`, and a 10-character prefix.
8. **No recurring subscription timer** — a periodic redraw keeps the grid's GPU
   buffers alive (the comment in `subscription()` records ~92 MB on macOS). Timers
   are one-shot and unsubscribed as soon as they fire.
9. **`ROW_HEIGHT` (22.0) is not the row pitch** — real pitch is `GRID_ROW_H`
   (23.6); `ROW_HEIGHT` is only used to seed the `Row(n)` scroll offset.
10. **Header / body scroll mirroring** — `GridScrolled` and `HeaderScrolled` both
    push an absolute offset to the other scroller. Keep the 0.5px guard, or the two
    will feed each other in a loop.
11. **Font family name** — `.SF NS Mono` is the only name that resolves on macOS.
    `SFMono-Regular` and `SF Mono` fall back to Helvetica via CoreText.
12. **Virtualized grid height** — the two spacers must sum to
    `(n - rendered) * GRID_PITCH_Y`, or the scrollbar and the rendered rows drift
    apart.

## macOS Open With: Parquet Opener helper

`bundle/opener.applescript` is compiled by `scripts/bundle_macos.sh` into
`Contents/Helpers/Parquet Opener.app`. The droplet owns the `.parquet` association
(`CFBundleDocumentTypes`, `LSItemContentTypes: org.apache.parquet`,
`LSHandlerRank: Owner`, bundle id `com.aruntaxak.parquet-viewer.opener`) and runs
`bundle/launch.sh` with the dropped files.

`launch.sh` resolves the viewer binary from its own path
(`${0%/Helpers/*}/MacOS/parquet-viewer`), `nohup`s one process per file, and exits,
so the droplet does not keep the viewer alive and vice versa. Because it always
executes the binary, each Open With starts a new process rather than reusing a
running one.

Cold launches need the droplet because the `odoc` Apple Event is dispatched before
the app can register a handler, and it goes to winit's `NSApplication` delegate,
which drops it. Warm opens while running are queued by the `FileOpenHandler`
(`src/main.rs:85`) registered from `Message::Started`, which handles both
`aevt/odoc` and `aevt/GURL`, extracts the path from `fileURLValue` or `stringValue`,
and pushes it through the `macos_open` `Subscription` as `Message::ExternalOpen`.

## Editing rules

- Keep `src/main.rs` single-file.
- Keep `PAGE_SIZE` / `TAIL_SIZE` at 1500 unless asked otherwise.
- Preserve the Tail default and its bottom scroll.
- Format cells in `rebuild_rows()`, not in `view()`.
- After touching the grid, re-check header/body column alignment while scrolling
  sideways, and the spacer heights while scrolling vertically.
- Run `cargo check` before `cargo build --release`.

## Look & feel

Adjust only when asked:

- `Theme::Light`; window `750x950`, minimum `700x500`
- app-wide `.SF NS Mono`, `UI_FONT` / `GRID_FONT` = 12.0, status text 11
- grid row height 23.6, 4px cell padding, 1px column lines
- toolbar button padding `[3.0, 6.0]`, spacing 4, bar padding 6
- button greys: `bar_button` 0.90 / hover 0.84 / pressed 0.77, `tab_active` 0.80
- dropdown block 0.90, menu background 0.97, scrollbar scroller 0.70
- drop overlay `rgba(0, 0, 0, 0.35)`
