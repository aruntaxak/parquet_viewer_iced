use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use arrow::array::{Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use chrono::Datelike;
use iced::widget::{
    button, column, container, mouse_area, pick_list, row, scrollable, space, stack, text,
};
use iced::{
    Alignment, Background, Border, Color, Element, Font, Length, Padding, Subscription, Task, Theme,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::reader::{FileReader, SerializedFileReader};
use regex::Regex;
use serde_json::{json, Value};

#[cfg(target_os = "macos")]
mod macos_open {
    use std::collections::VecDeque;
    use std::sync::{Mutex, OnceLock};
    use std::task::{Context, Poll, Waker};

    struct Pending {
        paths: VecDeque<String>,
        waker: Option<Waker>,
    }

    static PENDING: OnceLock<Mutex<Pending>> = OnceLock::new();
    fn pending() -> &'static Mutex<Pending> {
        PENDING.get_or_init(|| {
            Mutex::new(Pending {
                paths: VecDeque::new(),
                waker: None,
            })
        })
    }

    pub fn push_path(s: String) {
        let mut p = pending().lock().unwrap();
        p.paths.push_back(s);
        if let Some(waker) = p.waker.take() {
            waker.wake();
        }
    }

    /// Event-driven subscription: emits only when an AppleEvent delivers a path.
    /// No timer, so the window stays idle and the GPU memory is released.
    pub fn subscription() -> iced::Subscription<crate::Message> {
        iced::Subscription::run(stream)
    }

    fn stream() -> impl iced::futures::Stream<Item = crate::Message> + Send + 'static {
        iced::futures::stream::poll_fn(|cx: &mut Context<'_>| {
            let mut p = pending().lock().unwrap();
            if let Some(path) = p.paths.pop_front() {
                Poll::Ready(Some(crate::Message::ExternalOpen(path)))
            } else {
                p.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
    }

    use objc2_app_kit::NSResponder;
    use objc2::declare_class;
    use objc2::msg_send;
    use objc2::runtime::{AnyClass, AnyObject};
    use objc2::{ClassType, DeclaredClass};

    // FourCharCodes
    const K_AE_CORE: u32 = 0x61657674; // 'aevt'
    const K_AE_OPEN: u32 = 0x6F646F63; // 'odoc'
    const K_AE_GURL: u32 = 0x4755524C; // 'GURL'
    const KEY_DIRECT: u32 = 0x2D2D2D2D; // '----'


    // NSResponder subclass — receives Apple Events directly from NSAppleEventManager.
    // Registering here (without becoming the NSApplication delegate) keeps winit's
    // own delegate intact while still receiving warm odoc/GURL events.
    declare_class!(
        struct FileOpenHandler;

        unsafe impl ClassType for FileOpenHandler {
            type Super = NSResponder;
            type Mutability = objc2::mutability::MainThreadOnly;
            const NAME: &'static str = "ParquetFileOpenHandler";
        }

        impl DeclaredClass for FileOpenHandler {}

        unsafe impl FileOpenHandler {
            #[method(handleAppleEvent:withReplyEvent:)]
            fn handle(&self, event: &AnyObject, _reply: &AnyObject) {
                crate::debug_log("handler: handleAppleEvent");
                unsafe {
                    let list: *mut AnyObject = msg_send![event, paramDescriptorForKeyword: KEY_DIRECT];
                    if !list.is_null() {
                        let n: isize = msg_send![&*list, numberOfItems];
                        for i in 1..=n {
                            let desc: *mut AnyObject = msg_send![&*list, descriptorAtIndex: i];
                            if desc.is_null() { continue; }
                            // try fileURLValue -> NSURL -> path (UTF8String)
                            let url: *mut AnyObject = msg_send![&*desc, fileURLValue];
                            if !url.is_null() {
                                let ns_path: *mut AnyObject = msg_send![&*url, path];
                                if !ns_path.is_null() {
                                    let cstr: *const i8 = msg_send![&*ns_path, UTF8String];
                                    if !cstr.is_null() {
                                        let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                        push_path(s);
                                    }
                                }
                                continue;
                            }
                            let s_obj: *mut AnyObject = msg_send![&*desc, stringValue];
                            if !s_obj.is_null() {
                                let cstr: *const i8 = msg_send![&*s_obj, UTF8String];
                                if !cstr.is_null() {
                                    let st = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
                                    if st.starts_with("file://") {
                                        if let Ok(u) = url::Url::parse(&st) {
                                            if let Ok(p) = u.to_file_path() {
                                                push_path(p.to_string_lossy().to_string());
                                                continue;
                                            }
                                        }
                                    }
                                    if !st.is_empty() {
                                        push_path(st);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    );

    impl FileOpenHandler {
        fn new() -> *mut Self {
            let cls = Self::class();
            unsafe {
                let obj: *mut Self = msg_send![cls, alloc];
                // init is inherited from NSObject/NSResponder
                let obj: *mut Self = msg_send![obj, init];
                obj
            }
        }
    }

    static mut HANDLER: Option<*mut FileOpenHandler> = None;

    // Register our handler on NSAppleEventManager. Registered shortly after the
    // winit/iced event loop starts (see `Message::Started`). This covers warm
    // odoc/GURL opens while the app is running. Cold launches are handled by the
    // bundled Parquet Opener droplet, which forwards the path as argv.
    //
    // NOTE: do NOT replace NSApplication's delegate. winit 0.30 panics with
    // "tried to get a delegate that was not the one Winit has registered".
    pub fn install_apple_event_inner() {
        crate::debug_log("install_apple_event_inner");
        unsafe {
            let handler = FileOpenHandler::new();
            let sel = objc2::runtime::Sel::register("handleAppleEvent:withReplyEvent:");
            let cls = AnyClass::get("NSAppleEventManager").expect("NSAppleEventManager class");
            let mgr: *mut AnyObject = msg_send![cls, sharedAppleEventManager];
            let _: () = msg_send![
                &*mgr,
                setEventHandler: &*handler
                andSelector: sel
                forEventClass: K_AE_CORE
                andEventID: K_AE_OPEN
            ];
            let _: () = msg_send![
                &*mgr,
                setEventHandler: &*handler
                andSelector: sel
                forEventClass: K_AE_CORE
                andEventID: K_AE_GURL
            ];
            HANDLER = Some(handler);
        }
    }

}

const PAGE_SIZE: usize = 1500; // 24h traded instruments have 1440 rows/day (1m bars); 1500 covers a full day plus a bit of prev/next
#[allow(dead_code)]
const TAIL_SIZE: usize = 1500;
const ROW_HEIGHT: f32 = 22.0;
const GRID_ID: &str = "data_grid";
// Virtualized grid: only this many rows are built as real text; the rest of
// the page uses a shared one-space placeholder to keep the layout height.
const GRID_OVERSCAN: usize = 8;
const GRID_DEFAULT_ROWS: usize = 80;
const HEADER_ID: &str = "data_header";
const UI_FONT: f32 = 12.0;
const GRID_FONT: f32 = 12.0;
// No horizontal rules between rows; the header carries thin drag handles.
const GRID_ROW_H: f32 = 23.6;
const GRID_PITCH_Y: f32 = GRID_ROW_H;
const GRID_MIN_COL_W: f32 = 48.0;
const RESIZE_HANDLE_W: f32 = 5.0;
// Day and month fields only ever show two digits (max 31).
const DATE_FIELD_W: f32 = 38.0;
const YEAR_FIELD_W: f32 = 56.0;

// App-wide text font: SF Mono Regular, the font the terminal uses.
// macOS exposes it only as the variable family ".SF NS Mono"
// (/System/Library/Fonts/SFNSMono.ttf, wght axis 294.67..900). cosmic-text
// drives that axis from Font::weight, so Weight::Normal renders Regular.
const APP_FONT: Font = Font::with_name(".SF NS Mono");
const BOLD: Font = Font {
    weight: iced::font::Weight::Bold,
    ..APP_FONT
};
const MONO: Font = APP_FONT;

// Diagnostic logging for macOS Open-With path. Active only when PARQUET_DEBUG is set.
fn debug_log(msg: &str) {
    if std::env::var("PARQUET_DEBUG").is_ok() {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open("/tmp/parquet-viewer-debug.log") {
            let _ = writeln!(f, "[{}] {}", chrono::Local::now().format("%H:%M:%S%.3f"), msg);
        }
    }
}

fn resolve_path(p: &str) -> String {
    let mut s = p.trim().to_string();
    if s.starts_with("file://") {
        if let Ok(url) = url::Url::parse(&s) {
            if let Ok(path) = url.to_file_path() {
                s = path.to_string_lossy().to_string();
            }
        }
    }
    if s.starts_with("~/") {
        if let Some(home) = dirs::home_dir() {
            s = s.replacen("~/", &format!("{}/", home.display()), 1);
        }
    }
    let path = Path::new(&s);
    if path.exists() {
        if let Ok(can) = path.canonicalize() {
            return can.to_string_lossy().to_string();
        }
    }
    s
}

fn get_total_rows(path: &str) -> Result<usize, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    Ok(reader.metadata().file_metadata().num_rows() as usize)
}

fn get_columns(path: &str) -> Result<Vec<String>, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    let schema = reader.metadata().file_metadata().schema();
    Ok(schema.get_fields().iter().map(|f| f.name().to_string()).collect())
}

fn array_value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return String::new();
    }
    match array.data_type() {
        arrow::datatypes::DataType::Utf8 => array.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string(),
        arrow::datatypes::DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row).to_string(),
        arrow::datatypes::DataType::Int32 => array.as_any().downcast_ref::<Int32Array>().unwrap().value(row).to_string(),
        arrow::datatypes::DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().unwrap().value(row).to_string(),
        arrow::datatypes::DataType::Float32 => array.as_any().downcast_ref::<Float32Array>().unwrap().value(row).to_string(),
        arrow::datatypes::DataType::Float64 => array.as_any().downcast_ref::<Float64Array>().unwrap().value(row).to_string(),
        arrow::datatypes::DataType::Boolean => array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row).to_string(),
        // Explicit Timestamp arm: ISO via arrow's default display (e.g. 2026-10-02T13:35:00)
        arrow::datatypes::DataType::Timestamp(_, _) => array_fallback_to_string(array, row),
        _ => array_fallback_to_string(array, row),
    }
}

/// Last-resort renderer for types without an explicit fast path above
/// (Date64, Time32/64, Duration, UInt*, Decimal, Dictionary, nested, ...).
/// Uses arrow's ArrayFormatter with default (ISO) options so it renders the
/// SINGLE value at `row` — never the whole-array Debug dump that
/// `format!("{:?}", array)` produced.
fn array_fallback_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return String::new();
    }
    match ArrayFormatter::try_new(array, &FormatOptions::default()) {
        Ok(f) => f.value(row).to_string(),
        Err(_) => format!("<{}>", array.data_type()),
    }
}

fn array_to_json_value(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        arrow::datatypes::DataType::Utf8 => Value::String(array.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string()),
        arrow::datatypes::DataType::LargeUtf8 => Value::String(array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row).to_string()),
        arrow::datatypes::DataType::Int32 => json!(array.as_any().downcast_ref::<Int32Array>().unwrap().value(row)),
        arrow::datatypes::DataType::Int64 => json!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
        arrow::datatypes::DataType::Float32 => { let v = array.as_any().downcast_ref::<Float32Array>().unwrap().value(row); if v.is_finite() { json!(v) } else { Value::String(v.to_string()) } },
        arrow::datatypes::DataType::Float64 => { let v = array.as_any().downcast_ref::<Float64Array>().unwrap().value(row); if v.is_finite() { json!(v) } else { Value::String(v.to_string()) } },
        arrow::datatypes::DataType::Boolean => json!(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row)),
        arrow::datatypes::DataType::Date32 => json!(array.as_any().downcast_ref::<Int32Array>().unwrap().value(row)),
        // Explicit Timestamp arm (ISO via ArrayFormatter default), same fallback
        // for every other type so no column ever renders as a whole-array dump.
        arrow::datatypes::DataType::Timestamp(_, _) => Value::String(array_fallback_to_string(array, row)),
        _ => Value::String(array_fallback_to_string(array, row)),
    }
}

fn read_slice_columnar(path: &str, columns: &[String], offset: usize, length: usize) -> Result<HashMap<String, Vec<Value>>, String> {
    if length == 0 {
        let mut map = HashMap::new();
        for c in columns { map.insert(c.clone(), vec![]); }
        return Ok(map);
    }
    let file = File::open(path).map_err(|e| e.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;
    let parquet_schema = builder.parquet_schema();
    let file_meta = builder.metadata().file_metadata();
    let schema = file_meta.schema();
    let fields = schema.get_fields();
    let mut col_indices = Vec::new();
    for col in columns {
        if let Some(idx) = fields.iter().position(|f| f.name() == col) { col_indices.push(idx); }
    }
    let mask = ProjectionMask::roots(parquet_schema, col_indices);
    let batch_reader = builder.with_projection(mask).with_offset(offset).with_limit(length).with_batch_size(512).build().map_err(|e| e.to_string())?;
    let mut result: HashMap<String, Vec<Value>> = HashMap::new();
    for c in columns { result.insert(c.clone(), Vec::with_capacity(length)); }
    for batch_res in batch_reader {
        let batch = batch_res.map_err(|e| e.to_string())?;
        for col_idx in 0..batch.num_columns() {
            let col_name = columns[col_idx].clone();
            let array = batch.column(col_idx).as_ref();
            let vec = result.get_mut(&col_name).unwrap();
            for row in 0..batch.num_rows() { vec.push(array_to_json_value(array, row)); }
        }
    }
    Ok(result)
}

fn get_row_group_info(path: &str) -> Result<(usize, Vec<usize>), String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    let meta = reader.metadata();
    let num_rg = meta.num_row_groups();
    let mut counts = Vec::with_capacity(num_rg);
    for i in 0..num_rg { counts.push(meta.row_group(i).num_rows() as usize); }
    Ok((num_rg, counts))
}

// ---- Layout inspector (footer-only, local files, no network) ----
#[derive(Clone)]
struct LayoutChunk {
    name: String,
    phys: String,
    codec: String,
    encodings: String,
    num_values: i64,
    compressed: i64,
    uncompressed: i64,
    data_offset: i64,
    dict_offset: Option<i64>,
    index_offset: Option<i64>,
    stat_min: String,
    stat_max: String,
    null_count: Option<u64>,
    has_bloom: bool,
    has_col_index: bool,
    has_offset_index: bool,
}

#[derive(Clone)]
struct LayoutRG {
    idx: usize,
    num_rows: i64,
    total_bytes: i64,
    compressed_bytes: i64,
    chunks: Vec<LayoutChunk>,
}

#[derive(Clone)]
struct SchemaCol {
    name: String,
    phys: String,
    logical: String,
}

#[derive(Clone)]
struct LayoutInfo {
    file_size: u64,
    version: i32,
    num_rows: i64,
    num_rgs: usize,
    num_cols: usize,
    created_by: String,
    kv: Vec<(String, String)>,
    schema_cols: Vec<SchemaCol>,
    row_groups: Vec<LayoutRG>,
}

fn fmt_size(n: i64) -> String {
    let n = n.max(0) as u64;
    const U: &[&str] = &["B", "KB", "MB", "GB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < U.len() { v /= 1024.0; u += 1; }
    if u == 0 { format!("{} B", n) } else { format!("{:.1} {}", v, U[u]) }
}

fn fmt_size_u64(n: u64) -> String { fmt_size(n as i64) }

fn short_str(s: &str, max: usize) -> String {
    if s.len() <= max { s.to_string() } else { format!("{}…", &s[..max]) }
}

fn ba_to_string(data: &[u8]) -> String {
    if data.is_empty() { return String::new(); }
    let s = String::from_utf8_lossy(data);
    // if mostly printable, show as string else hex
    let printable = s.chars().filter(|c| !c.is_control() || *c == '\n' || *c == '\t').count();
    if printable * 2 >= s.len() {
        short_str(&s, 32)
    } else {
        let hex: String = data.iter().take(16).map(|b| format!("{:02x}", b)).collect();
        format!("0x{}", hex)
    }
}

fn stat_strings(s: Option<&parquet::file::statistics::Statistics>) -> (String, String, Option<u64>) {
    use parquet::file::statistics::Statistics as S;
    match s {
        None => ("—".to_string(), "—".to_string(), None),
        Some(st) => {
            let nulls = st.null_count_opt();
            let (mn, mx) = match st {
                S::Boolean(v) => (
                    v.min_opt().map(|x| x.to_string()).unwrap_or_default(),
                    v.max_opt().map(|x| x.to_string()).unwrap_or_default(),
                ),
                S::Int32(v) => (
                    v.min_opt().map(|x| x.to_string()).unwrap_or_default(),
                    v.max_opt().map(|x| x.to_string()).unwrap_or_default(),
                ),
                S::Int64(v) => (
                    v.min_opt().map(|x| x.to_string()).unwrap_or_default(),
                    v.max_opt().map(|x| x.to_string()).unwrap_or_default(),
                ),
                S::Float(v) => (
                    v.min_opt().map(|x| x.to_string()).unwrap_or_default(),
                    v.max_opt().map(|x| x.to_string()).unwrap_or_default(),
                ),
                S::Double(v) => (
                    v.min_opt().map(|x| x.to_string()).unwrap_or_default(),
                    v.max_opt().map(|x| x.to_string()).unwrap_or_default(),
                ),
                S::ByteArray(v) => (
                    v.min_opt().map(|b| ba_to_string(b.data())).unwrap_or_default(),
                    v.max_opt().map(|b| ba_to_string(b.data())).unwrap_or_default(),
                ),
                S::FixedLenByteArray(v) => (
                    v.min_opt().map(|b| ba_to_string(b.data())).unwrap_or_default(),
                    v.max_opt().map(|b| ba_to_string(b.data())).unwrap_or_default(),
                ),
                S::Int96(v) => (
                    v.min_opt().map(|x| format!("{}", x)).unwrap_or_default(),
                    v.max_opt().map(|x| format!("{}", x)).unwrap_or_default(),
                ),
            };
            (mn, mx, nulls)
        }
    }
}

fn get_layout_info(path: &str) -> Result<LayoutInfo, String> {
    let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let file = File::open(path).map_err(|e| e.to_string())?;
    let reader = SerializedFileReader::new(file).map_err(|e| e.to_string())?;
    let meta = reader.metadata();
    let fmeta = meta.file_metadata();
    let version = fmeta.version();
    let num_rows = fmeta.num_rows();
    let created_by = fmeta.created_by().unwrap_or("—").to_string();
    let kv: Vec<(String, String)> = fmeta
        .key_value_metadata()
        .map(|v| v.iter().map(|k| (k.key.clone(), k.value.clone().unwrap_or_default())).collect())
        .unwrap_or_default();
    // schema (leaf columns only)
    let descr = fmeta.schema_descr();
    let mut schema_cols = Vec::new();
    for c in descr.columns() {
        let logical = c.logical_type().map(|l| format!("{:?}", l)).unwrap_or_else(|| format!("{:?}", c.converted_type()));
        schema_cols.push(SchemaCol {
            name: c.path().string(),
            phys: format!("{}", c.physical_type()),
            logical,
        });
    }
    let num_rgs = meta.num_row_groups();
    let mut row_groups = Vec::with_capacity(num_rgs);
    for i in 0..num_rgs {
        let rg = meta.row_group(i);
        let mut chunks = Vec::new();
        for cc in rg.columns() {
            let (mn, mx, nulls) = stat_strings(cc.statistics());
            let enc = cc.encodings().iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(",");
            chunks.push(LayoutChunk {
                name: cc.column_path().string(),
                phys: format!("{}", cc.column_type()),
                codec: format!("{}", cc.compression()),
                encodings: enc,
                num_values: cc.num_values(),
                compressed: cc.compressed_size(),
                uncompressed: cc.uncompressed_size(),
                data_offset: cc.data_page_offset(),
                dict_offset: cc.dictionary_page_offset(),
                index_offset: cc.index_page_offset(),
                stat_min: mn,
                stat_max: mx,
                null_count: nulls,
                has_bloom: cc.bloom_filter_offset().is_some(),
                has_col_index: cc.column_index_offset().is_some(),
                has_offset_index: cc.offset_index_offset().is_some(),
            });
        }
        row_groups.push(LayoutRG {
            idx: i,
            num_rows: rg.num_rows(),
            total_bytes: rg.total_byte_size(),
            compressed_bytes: rg.compressed_size(),
            chunks,
        });
    }
    Ok(LayoutInfo {
        file_size,
        version,
        num_rows,
        num_rgs,
        num_cols: schema_cols.len(),
        created_by,
        kv,
        schema_cols,
        row_groups,
    })
}

// ---- UI state ----

#[derive(Debug, Clone, Copy)]
enum ScrollTarget {
    Top,
    Bottom,
    Row(usize),
}

#[derive(Debug, Clone, Copy)]
struct ResizeState {
    col: usize,
    start_x: Option<f32>,
    left: f32,
    right: f32,
}

#[derive(Debug, Clone)]
enum Message {
    OpenDialog,
    FilePicked(Option<std::path::PathBuf>),
    WindowEvent(iced::window::Event),
    GridScrolled {
        x: f32,
        y: f32,
        viewport_h: f32,
    },
    HeaderScrolled {
        x: f32,
    },
    ResizeStart(usize),
    ResizeMove(f32),
    ResizeEnd,
    Started,
    AutoOpen,
    ExternalOpen(String),
    ToggleShowAll,
    PrevPage,
    NextPage,
    ShowData,
    ShowLayout,
    ToggleSchema,
    ToggleKv,
    ToggleRowGroup(usize),
    YearPicked(i32),
    MonthPicked(u32),
    DayPicked(u32),
    GoToDate,
}

struct ParquetViewerApp {
    path: Option<String>,
    total_rows: usize,
    columns: Vec<String>,
    data: HashMap<String, Vec<Value>>,
    rows: Vec<Rc<Vec<String>>>,
    grid_window: std::ops::Range<usize>,
    grid_offset_x: f32,
    col_widths: Vec<f32>,
    resizing: Option<ResizeState>,
    window_width: f32,
    window_height: f32,
    current_page: usize,
    total_pages: usize,
    show_all: bool,
    show_layout: bool,
    layout: Option<LayoutInfo>,
    status: String,
    scroll: Option<ScrollTarget>,
    sel_year: i32,
    sel_month: u32,
    sel_day: u32,
    auto_open_done: bool,
    started: bool,
    hovering: bool,
    schema_open: bool,
    kv_open: bool,
    expanded_rgs: HashSet<usize>,
}

impl Default for ParquetViewerApp {
    fn default() -> Self {
        let today = chrono::Local::now().date_naive();
        Self {
            path: None,
            total_rows: 0,
            columns: vec![],
            data: HashMap::new(),
            rows: vec![],
            grid_window: 0..GRID_DEFAULT_ROWS,
            grid_offset_x: 0.0,
            col_widths: vec![],
            resizing: None,
            window_width: 750.0,
            window_height: 950.0,
            current_page: 0,
            total_pages: 1,
            show_all: false,
            show_layout: false,
            layout: None,
            status: "No file loaded - choose a parquet file".to_string(),
            scroll: None,
            sel_year: today.year(),
            sel_month: today.month(),
            sel_day: today.day(),
            auto_open_done: false,
            started: false,
            hovering: false,
            schema_open: true,
            kv_open: false,
            expanded_rgs: HashSet::new(),
        }
    }
}

fn cell_text(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string().trim_matches('"').to_string(),
    }
}

/// Date/time columns are left aligned; every other column is centered.
fn column_alignment(name: &str) -> Alignment {
    let n = name.trim().to_ascii_lowercase();
    match n.as_str() {
        "date" | "time" | "datetime" => Alignment::Start,
        _ => Alignment::Center,
    }
}

fn grid_cell<'a>(
    content: &'a str,
    bold: bool,
    width: f32,
    align: Alignment,
) -> Element<'a, Message> {
    container(
        text(content)
            .size(GRID_FONT)
            .font(if bold { BOLD } else { APP_FONT })
            .wrapping(iced::widget::text::Wrapping::None),
    )
    .width(Length::Fixed(width))
    .height(Length::Fixed(GRID_ROW_H))
    .padding([0.0, 4.0])
    .align_x(align)
    .align_y(Alignment::Center)
    .into()
}

fn column_line<'a>(height: f32) -> Element<'a, Message> {
    container(space())
        .width(Length::Fixed(1.0))
        .height(Length::Fixed(height))
        .style(|theme: &Theme| container::Style {
            background: Some(Background::Color(
                theme.extended_palette().background.strong.color,
            )),
            ..container::Style::default()
        })
        .into()
}

fn grid_row<'a>(
    cells: impl Iterator<Item = &'a str>,
    widths: &[f32],
    aligns: &[Alignment],
    bold: bool,
) -> Element<'a, Message> {
    let mut children: Vec<Element<'a, Message>> = Vec::new();
    for (i, content) in cells.enumerate() {
        if i > 0 {
            children.push(column_line(GRID_ROW_H));
        }
        children.push(grid_cell(
            content,
            bold,
            widths.get(i).copied().unwrap_or(GRID_MIN_COL_W),
            aligns.get(i).copied().unwrap_or(Alignment::Center),
        ));
    }
    row(children).spacing(0).into()
}

fn grid_header_row<'a>(columns: &'a [String], widths: &[f32]) -> Element<'a, Message> {
    let mut children: Vec<Element<'a, Message>> = Vec::with_capacity(columns.len() * 2);
    for (i, content) in columns.iter().enumerate() {
        if i > 0 {
            children.push(column_line(GRID_ROW_H));
        }
        let width = widths.get(i).copied().unwrap_or(GRID_MIN_COL_W);
        let cell = grid_cell(content, true, width, column_alignment(content));
        if i + 1 == columns.len() {
            children.push(cell);
            continue;
        }
        let handle = container(space())
            .width(Length::Fixed(RESIZE_HANDLE_W))
            .height(Length::Fixed(GRID_ROW_H));
        let overlay = container(mouse_area(handle).on_press(Message::ResizeStart(i)))
            .width(Length::Fixed(width))
            .height(Length::Fixed(GRID_ROW_H))
            .align_x(Alignment::End);
        children.push(stack![cell, overlay].into());
    }
    row(children).spacing(0).into()
}

/// Scrollbars stay grey in every state (the default iced style turns the
/// scroller blue when it is hovered or dragged).
fn grey_scrollable_style(theme: &Theme, status: scrollable::Status) -> scrollable::Style {
    let mut style = scrollable::default(theme, status);
    let scroller = scrollable::Scroller {
        background: Background::Color(Color::from_rgb(0.70, 0.70, 0.70)),
        border: Border {
            radius: 2.0.into(),
            ..Border::default()
        },
    };
    style.vertical_rail.scroller = scroller;
    style.horizontal_rail.scroller = scroller;
    style
}

/// Closed dropdown fields: same solid light grey block as the menu bar
/// buttons, so they reach the rules above and below.
fn dropdown_style(_theme: &Theme, _status: pick_list::Status) -> pick_list::Style {
    pick_list::Style {
        text_color: Color::BLACK,
        placeholder_color: Color::from_rgb(0.45, 0.45, 0.45),
        handle_color: Color::from_rgb(0.35, 0.35, 0.35),
        background: Background::Color(Color::from_rgb(0.90, 0.90, 0.90)),
        border: Border {
            radius: 4.0.into(),
            width: 0.0,
            color: Color::TRANSPARENT,
        },
    }
}

/// Open dropdown list: grey highlight instead of the blue default.
fn dropdown_menu_style(_theme: &Theme) -> iced::overlay::menu::Style {
    iced::overlay::menu::Style {
        background: Background::Color(Color::from_rgb(0.97, 0.97, 0.97)),
        border: Border {
            radius: 4.0.into(),
            width: 1.0,
            color: Color::from_rgb(0.78, 0.78, 0.78),
        },
        text_color: Color::BLACK,
        selected_text_color: Color::BLACK,
        selected_background: Background::Color(Color::from_rgb(0.80, 0.80, 0.80)),
        shadow: iced::Shadow::default(),
    }
}

fn bar_button(_theme: &Theme, status: button::Status) -> button::Style {
    let (background, text_color) = match status {
        button::Status::Hovered => (Color::from_rgb(0.84, 0.84, 0.84), Color::BLACK),
        button::Status::Pressed => (Color::from_rgb(0.77, 0.77, 0.77), Color::BLACK),
        button::Status::Disabled => (
            Color::from_rgb(0.94, 0.94, 0.94),
            Color::from_rgb(0.55, 0.55, 0.55),
        ),
        button::Status::Active => (Color::from_rgb(0.90, 0.90, 0.90), Color::BLACK),
    };
    button::Style {
        background: Some(Background::Color(background)),
        text_color,
        border: Border {
            radius: 4.0.into(),
            ..Border::default()
        },
        ..button::Style::default()
    }
}

fn tab_active(_theme: &Theme, status: button::Status) -> button::Style {
    let (background, text_color) = match status {
        button::Status::Hovered => (Color::from_rgb(0.74, 0.74, 0.74), Color::BLACK),
        button::Status::Pressed => (Color::from_rgb(0.68, 0.68, 0.68), Color::BLACK),
        button::Status::Disabled => (
            Color::from_rgb(0.85, 0.85, 0.85),
            Color::from_rgb(0.45, 0.45, 0.45),
        ),
        button::Status::Active => (Color::from_rgb(0.80, 0.80, 0.80), Color::BLACK),
    };
    button::Style {
        background: Some(Background::Color(background)),
        text_color,
        border: Border {
            radius: 4.0.into(),
            ..Border::default()
        },
        ..button::Style::default()
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap { 29 } else { 28 }
        }
        _ => 31,
    }
}

impl ParquetViewerApp {
    fn boot(initial: Option<String>) -> Self {
        let mut app = Self::default();
        if let Some(p) = initial {
            app.open_path(&p);
            app.auto_open_done = true; // CLI file provided, don't auto-open
        }
        app
    }

    fn pick_file_task() -> Task<Message> {
        Task::perform(
            async {
                rfd::FileDialog::new()
                    .add_filter("Parquet", &["parquet"])
                    .add_filter("All", &["*"])
                    .pick_file()
            },
            Message::FilePicked,
        )
    }

    fn virtual_rows(&self) -> usize {
        self.total_rows
    }

    /// Number of rows to build as real text: the visible viewport plus
    /// overscan. The window is recomputed exactly on scroll; this estimate is
    /// used on page load and resize, before the first scroll event.
    fn grid_capacity(&self) -> usize {
        ((self.window_height / 20.0).ceil() as usize).max(1) + 2 * GRID_OVERSCAN
    }

    /// Columns share the full window width equally until the user drags a
    /// header divider. Handles keep the pair's total constant, so the table
    /// still fills the viewport.
    fn reset_column_widths(&mut self) {
        let n = self.columns.len();
        if n == 0 {
            self.col_widths.clear();
            return;
        }
        let separators = n.saturating_sub(1) as f32;
        let total = (self.window_width - separators).max(320.0);
        let width = (total / n as f32).max(GRID_MIN_COL_W);
        self.col_widths = vec![width; n];
    }

    fn update_total_pages(&mut self) {
        let vr = self.virtual_rows();
        self.total_pages = if vr == 0 { 1 } else { (vr + PAGE_SIZE - 1) / PAGE_SIZE };
        if self.current_page >= self.total_pages { self.current_page = self.total_pages.saturating_sub(1); }
    }

    fn open_path(&mut self, path_str: &str) {
        debug_log(&format!("open_path: {}", path_str));
        let rp = resolve_path(path_str);
        if !Path::new(&rp).is_file() {
            self.status = format!("File not found: {}", rp);
            return;
        }
        match get_columns(&rp) {
            Ok(cols) => {
                self.columns = cols;
                self.reset_column_widths();
            }
            Err(e) => { self.status = format!("Failed to get columns: {}", e); return; }
        }
        match get_total_rows(&rp) {
            Ok(n) => self.total_rows = n,
            Err(e) => { self.status = format!("Failed to get rows: {}", e); return; }
        }
        self.path = Some(rp.clone());
        self.update_total_pages();
        // Footer-only layout inspect (cheap, no data pages read)
        self.layout = get_layout_info(&rp).ok();
        self.expanded_rgs = match &self.layout {
            Some(l) if l.num_rgs <= 4 => (0..l.num_rgs).collect(),
            _ => HashSet::new(),
        };
        // Default to last page (Tail) when show_all is false, so Prev works immediately and Next is at end
        let start_page = if self.show_all { 0 } else { self.total_pages.saturating_sub(1) };
        self.current_page = start_page;
        self.status = format!("{} rows x {} cols", self.total_rows, self.columns.len());
        self.load_page(start_page);
    }

    fn rebuild_rows(&mut self) {
        let n = self.columns.first().and_then(|c| self.data.get(c)).map(|v| v.len()).unwrap_or(0);
        self.rows = (0..n)
            .map(|i| {
                Rc::new(
                    self.columns
                        .iter()
                        .map(|c| cell_text(self.data.get(c).and_then(|v| v.get(i))))
                        .collect(),
                )
            })
            .collect();
    }

    fn load_page(&mut self, page: usize) {
        if self.path.is_none() || self.columns.is_empty() { return; }
        let path = self.path.clone().unwrap();
        let vr = self.virtual_rows();
        let mut p = page;
        if p >= self.total_pages { p = self.total_pages.saturating_sub(1); }
        self.current_page = p;
        // Both Tail and Show all paginate full file; Tail just starts at last page
        let rs = p * PAGE_SIZE;
        let len = std::cmp::min(PAGE_SIZE, vr.saturating_sub(rs));
        if len == 0 { self.data.clear(); self.rows.clear(); return; }
        match read_slice_columnar(&path, &self.columns, rs, len) {
            Ok(d) => {
                self.data = d;
                self.rebuild_rows();
                if !self.show_all && p == self.total_pages.saturating_sub(1) {
                    self.scroll = Some(ScrollTarget::Bottom);
                } else {
                    self.scroll = Some(ScrollTarget::Top);
                }
                let cap = self.grid_capacity();
                let n = self.rows.len();
                self.grid_window = if matches!(self.scroll, Some(ScrollTarget::Bottom)) {
                    n.saturating_sub(cap)..n
                } else {
                    0..cap.min(n)
                };
            }
            Err(e) => { self.status = format!("Failed to load page: {}", e); self.data.clear(); self.rows.clear(); }
        }
    }

    // Load a window of up to PAGE_SIZE rows starting at `start` (not page-aligned).
    // Used by goto_date to place the first row of the matched date as the 2nd row
    // from the top by including one row of context before it.
    fn load_at(&mut self, start: usize) {
        if self.path.is_none() || self.columns.is_empty() { return; }
        let path = self.path.clone().unwrap();
        let vr = self.virtual_rows();
        let rs = start.min(vr);
        let len = std::cmp::min(PAGE_SIZE, vr.saturating_sub(rs));
        if len == 0 { self.data.clear(); self.rows.clear(); return; }
        let page = rs / PAGE_SIZE;
        self.current_page = page;
        match read_slice_columnar(&path, &self.columns, rs, len) {
            Ok(d) => {
                self.data = d;
                self.rebuild_rows();
                self.scroll = Some(ScrollTarget::Row(1));
                self.grid_window = 0..self.grid_capacity().min(self.rows.len());
            }
            Err(e) => { self.status = format!("Failed to load page: {}", e); self.data.clear(); self.rows.clear(); }
        }
    }

    fn get_date_column(&self) -> String {
        for c in &self.columns { if c.to_lowercase().contains("date") { return c.clone(); } }
        self.columns.first().cloned().unwrap_or_default()
    }

    fn goto_date(&mut self, date_str: &str) {
        if self.path.is_none() { self.status = "No file loaded".to_string(); return; }
        let date_str = date_str.trim().to_string();
        if date_str.is_empty() { self.status = "Empty date".to_string(); return; }
        let path = self.path.clone().unwrap();
        let cols = self.columns.clone();
        let date_col = self.get_date_column();
        if date_col.is_empty() { self.status = "No columns".to_string(); return; }
        let date_col_idx = cols.iter().position(|c| c == &date_col).unwrap_or(0);
        // sample 5 rows to detect format
        let sample = read_slice_columnar(&path, &cols, 0, 5).unwrap_or_default();
        let col_format = {
            let mut fmt = None;
            if let Some(vals) = sample.get(&date_col) {
                let re_dd = Regex::new(r"^\d{2}-\d{2}-\d{4}").unwrap();
                let re_slash = Regex::new(r"^\d{2}/\d{2}/\d{4}").unwrap();
                let re_yyyy = Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap();
                for v in vals {
                    let vs = match v { Value::String(s) => s.clone(), _ => v.to_string().trim_matches('"').to_string() };
                    if re_dd.is_match(&vs) { fmt = Some("dd-mm-yyyy"); break; }
                    if re_slash.is_match(&vs) { fmt = Some("dd/mm/yyyy"); break; }
                    if re_yyyy.is_match(&vs) { fmt = Some("yyyy-mm-dd"); break; }
                }
            }
            fmt
        };
        // Binance klines store the candle open time as an int64 Unix epoch, so no
        // date string is ever a substring of the cell text ("1714521600"). Those
        // columns are matched numerically instead: the unit comes from the digit
        // count (10 = seconds, 13 = milliseconds, 16 = microseconds). Any other
        // width, and every text column, keeps the variant matching below.
        let epoch_multiplier: Option<i128> = sample.get(&date_col).and_then(|vals| {
            let mut mult: Option<i128> = None;
            for v in vals {
                let vs = match v { Value::String(s) => s.clone(), _ => v.to_string().trim_matches('"').to_string() };
                if vs.is_empty() || !vs.bytes().all(|b| b.is_ascii_digit()) { return None; }
                let m: i128 = match vs.len() { 10 => 1, 13 => 1_000, 16 => 1_000_000, _ => return None };
                if mult.is_some_and(|prev| prev != m) { return None; }
                mult = Some(m);
            }
            mult
        });
        // UTC day bounds for an epoch column, expressed in that column's unit.
        let epoch_bounds: Option<(i128, i128)> = epoch_multiplier.and_then(|mult| {
            let day = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").ok()?;
            let start = day.and_hms_opt(0, 0, 0)?.and_utc().timestamp() as i128;
            let end = (day + chrono::Duration::days(1)).and_hms_opt(0, 0, 0)?.and_utc().timestamp() as i128;
            Some((start * mult, end * mult))
        });
        let mut variants: Vec<String> = Vec::new();
        let mut add = |v: String| { if !v.is_empty() && !variants.contains(&v) { variants.push(v); } };
        add(date_str.clone());
        let re_yyyy_dash = Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap();
        let re_yyyy_slash = Regex::new(r"^\d{4}/\d{2}/\d{2}").unwrap();
        let re_dd_dash = Regex::new(r"^\d{2}-\d{2}-\d{4}").unwrap();
        let re_dd_slash = Regex::new(r"^\d{2}/\d{2}/\d{4}").unwrap();
        if re_yyyy_dash.is_match(&date_str) {
            let p: Vec<&str> = date_str.split('-').collect();
            if p.len()==3 { let (y,m,d)=(p[0],p[1],p[2]); add(format!("{}-{}-{}",d,m,y)); add(format!("{}/{}/{}",d,m,y)); add(format!("{}/{}/{}",y,m,d)); }
        } else if re_yyyy_slash.is_match(&date_str) {
            let p: Vec<&str> = date_str.split('/').collect();
            if p.len()==3 { let (y,m,d)=(p[0],p[1],p[2]); add(format!("{}-{}-{}",d,m,y)); add(format!("{}/{}/{}",d,m,y)); add(format!("{}-{}-{}",y,m,d)); }
        } else if re_dd_dash.is_match(&date_str) {
            let p: Vec<&str> = date_str.split('-').collect();
            if p.len()==3 { let (d,m,y)=(p[0],p[1],p[2]); add(format!("{}-{}-{}",y,m,d)); add(format!("{}/{}/{}",d,m,y)); add(format!("{}/{}/{}",y,m,d)); }
        } else if re_dd_slash.is_match(&date_str) {
            let p: Vec<&str> = date_str.split('/').collect();
            if p.len()==3 { let (d,m,y)=(p[0],p[1],p[2]); add(format!("{}-{}-{}",y,m,d)); add(format!("{}-{}-{}",d,m,y)); add(format!("{}/{}/{}",y,m,d)); }
        }
        let mut prioritized = Vec::new();
        let mut others = Vec::new();
        if let Some(fmt) = col_format {
            for v in variants {
                let is_prio = match fmt { "dd-mm-yyyy" => re_dd_dash.is_match(&v), "dd/mm/yyyy" => re_dd_slash.is_match(&v), "yyyy-mm-dd" => re_yyyy_dash.is_match(&v), _ => false };
                if is_prio { prioritized.push(v); } else { others.push(v); }
            }
            variants = prioritized; variants.extend(others);
        }
        // scan
        let (num_rg, _) = match get_row_group_info(&path) { Ok(v) => v, Err(e) => { self.status = e; return; } };
        let mut global_idx: usize = 0;
        let mut found_idx: Option<usize> = None;
        for rg in 0..num_rg {
            if found_idx.is_some() { break; }
            let file = match File::open(&path) { Ok(f) => f, Err(_) => break };
            let builder = match ParquetRecordBatchReaderBuilder::try_new(file) { Ok(b) => b, Err(_) => break };
            let parquet_schema = builder.parquet_schema();
            let file_meta = builder.metadata().file_metadata();
            let fields = file_meta.schema().get_fields();
            let col_idx = fields.iter().position(|f| f.name() == &date_col).unwrap_or(date_col_idx);
            let mask = ProjectionMask::roots(parquet_schema, vec![col_idx]);
            let reader = match builder.with_projection(mask).with_row_groups(vec![rg]).with_batch_size(8192).build() { Ok(r) => r, Err(_) => continue };
            for batch_res in reader {
                let batch = match batch_res { Ok(b) => b, Err(_) => continue };
                let arr = batch.column(0).as_ref();
                for row in 0..batch.num_rows() {
                    let val_str = array_value_to_string(arr, row);
                    let hit = match epoch_bounds {
                        // [start, end) in the column's unit; a day's candles are
                        // matched without converting every cell to a timestamp.
                        Some((lo, hi)) => val_str.parse::<i128>().map(|n| n >= lo && n < hi).unwrap_or(false),
                        None => variants.iter().any(|v| val_str == *v || val_str.contains(v) || (val_str.len()>=10 && v.len()>=10 && &val_str[0..10]==&v[0..10])),
                    };
                    if hit {
                        found_idx = Some(global_idx);
                        break;
                    }
                    global_idx += 1;
                }
                if found_idx.is_some() { break; }
            }
        }
        if let Some(real_idx) = found_idx {
            // Load starting one row before the match so the first row of the
            // matched date appears as the 2nd row from the top (with the
            // previous row as context above it). saturating_sub handles the
            // edge case where the match is at the very first row of the file.
            self.load_at(real_idx.saturating_sub(1));
            self.status = format!("Found at row {} page {}", real_idx, real_idx / PAGE_SIZE + 1);
        } else {
            self.status = match epoch_bounds {
                Some((lo, hi)) => format!("Date '{}' not found in '{}' (epoch {}..{})", date_str, date_col, lo, hi),
                None => format!("Date '{}' not found in '{}' (tried {})", date_str, date_col, variants.join(", ")),
            };
        }
    }

    // Turn the pending scroll target into a widget operation. Runs after the
    // view has laid out the new page.
    fn take_scroll_task(&mut self) -> Task<Message> {
        match self.scroll.take() {
            None => Task::none(),
            Some(ScrollTarget::Top) => iced::widget::operation::scroll_to(
                GRID_ID,
                iced::widget::operation::AbsoluteOffset { x: Some(0.0), y: Some(0.0) },
            ),
            Some(ScrollTarget::Bottom) => iced::widget::operation::snap_to(
                GRID_ID,
                iced::widget::operation::RelativeOffset { x: 0.0, y: 1.0 },
            ),
            Some(ScrollTarget::Row(i)) => iced::widget::operation::scroll_to(
                GRID_ID,
                iced::widget::operation::AbsoluteOffset { x: Some(0.0), y: Some(i as f32 * ROW_HEIGHT) },
            ),
        }
    }

    fn view_toolbar(&self) -> Element<'_, Message> {
        // One compact row: every control is sized to fit the 750px window.
        let mut controls = row![
            button(text("Open").size(UI_FONT))
                .padding([3.0, 6.0])
                .style(bar_button)
                .on_press(Message::OpenDialog),
            button(
                text(if self.show_all { "Tail 1500" } else { "Show all" })
                    .size(UI_FONT),
            )
            .padding([3.0, 6.0])
            .style(bar_button)
            .on_press(Message::ToggleShowAll),
            button(text("Data").size(UI_FONT))
                .padding([3.0, 6.0])
                .style(if self.show_layout { bar_button } else { tab_active })
                .on_press(Message::ShowData),
            button(text("Layout").size(UI_FONT))
                .padding([3.0, 6.0])
                .style(if self.show_layout { tab_active } else { bar_button })
                .on_press(Message::ShowLayout),
        ]
        .spacing(4)
        .align_y(Alignment::Center);

        let can_prev = self.total_pages > 1 && self.current_page > 0;
        let prev = button(text("Prev").size(UI_FONT))
            .padding([3.0, 6.0])
            .style(bar_button);
        controls = controls.push(if can_prev {
            prev.on_press(Message::PrevPage)
        } else {
            prev
        });
        controls = controls.push(
            text(format!("Page {}/{}", self.current_page + 1, self.total_pages))
                .size(UI_FONT),
        );
        let can_next = self.total_pages > 1 && self.current_page + 1 < self.total_pages;
        let next = button(text("Next").size(UI_FONT))
            .padding([3.0, 6.0])
            .style(bar_button);
        controls = controls.push(if can_next {
            next.on_press(Message::NextPage)
        } else {
            next
        });

        let years: Vec<i32> = (2010..=chrono::Local::now().date_naive().year()).collect();
        let months: Vec<u32> = (1..=12).collect();
        let days: Vec<u32> = (1..=days_in_month(self.sel_year, self.sel_month)).collect();
        controls = controls
            .push(text("Go to Date").size(UI_FONT))
            .push(
                pick_list(days, Some(self.sel_day), Message::DayPicked)
                    .text_size(UI_FONT)
                    .padding([3.0, 4.0])
                    .width(DATE_FIELD_W)
                    .style(dropdown_style)
                    .menu_style(dropdown_menu_style),
            )
            .push(
                pick_list(months, Some(self.sel_month), Message::MonthPicked)
                    .text_size(UI_FONT)
                    .padding([3.0, 4.0])
                    .width(DATE_FIELD_W)
                    .style(dropdown_style)
                    .menu_style(dropdown_menu_style),
            )
            .push(
                pick_list(years, Some(self.sel_year), Message::YearPicked)
                    .text_size(UI_FONT)
                    .padding([3.0, 4.0])
                    .width(YEAR_FIELD_W)
                    .style(dropdown_style)
                    .menu_style(dropdown_menu_style),
            )
            .push(
                button(text("Go").size(UI_FONT))
                    .padding([3.0, 6.0])
                    .style(bar_button)
                    .on_press(Message::GoToDate),
            )
            .push(
                container(text(&self.status).size(11))
                    .width(Length::Fill)
                    .clip(true),
            );

        let bar = container(controls)
            .padding(Padding {
                top: 0.0,
                right: 6.0,
                bottom: 0.0,
                left: 6.0,
            })
            .width(Length::Fill);

        column![
            bar,
            container(space())
                .width(Length::Fill)
                .height(Length::Fixed(1.0))
                .style(|theme: &Theme| container::Style {
                    background: Some(Background::Color(
                        theme.extended_palette().background.strong.color,
                    )),
                    ..container::Style::default()
                }),
        ]
        .spacing(0)
        .into()
    }

    fn view_data(&self) -> Element<'_, Message> {
        if self.columns.is_empty() {
            return container(text("No file loaded - drag & drop parquet here"))
                .center(Length::Fill)
                .into();
        }
        let n = self.rows.len();
        let first = self.grid_window.start.min(n);
        let last = self.grid_window.end.min(n).max(first);

        // Column header stays fixed above the body. It lives in its own
        // horizontal scroller whose offset is mirrored from the body, so the
        // columns stay aligned when scrolling sideways.
        let header = grid_header_row(&self.columns, &self.col_widths);
        let header = scrollable(header)
            .id(HEADER_ID)
            .direction(scrollable::Direction::Horizontal(
                scrollable::Scrollbar::hidden(),
            ))
            .on_scroll(|v| Message::HeaderScrolled {
                x: v.absolute_offset().x,
            })
            .width(Length::Fill)
            .height(Length::Fixed(GRID_ROW_H));

        let aligns: Vec<Alignment> =
            self.columns.iter().map(|c| column_alignment(c)).collect();

        let mut body_children: Vec<Element<'_, Message>> =
            Vec::with_capacity(last - first);
        for row in &self.rows[first..last] {
            body_children.push(grid_row(
                row.iter().map(String::as_str),
                &self.col_widths,
                &aligns,
                false,
            ));
        }

        let body = column![
            space().height(first as f32 * GRID_PITCH_Y),
            column(body_children).spacing(0),
            space().height((n - last) as f32 * GRID_PITCH_Y),
        ]
        .spacing(0);

        let body = scrollable(body)
            .id(GRID_ID)
            .direction(scrollable::Direction::Both {
                vertical: scrollable::Scrollbar::new(),
                horizontal: scrollable::Scrollbar::new(),
            })
            .style(grey_scrollable_style)
            .on_scroll(|v| {
                let offset = v.absolute_offset();
                Message::GridScrolled {
                    x: offset.x,
                    y: offset.y,
                    viewport_h: v.bounds().height,
                }
            })
            .width(Length::Fill)
            .height(Length::Fill);

        column![header, body]
            .spacing(0)
            .height(Length::Fill)
            .into()
    }

    fn view_layout(&self) -> Element<'_, Message> {
        let Some(l) = &self.layout else {
            return container(text("Layout unavailable for this file."))
                .center(Length::Fill)
                .into();
        };

        let info = column![
            text("File").size(15).font(BOLD),
            info_row("Size:", fmt_size_u64(l.file_size)),
            info_row("Version:", format!("v{}", l.version)),
            info_row("Rows:", format!("{}", l.num_rows)),
            info_row("Row groups:", format!("{}", l.num_rgs)),
            info_row("Columns:", format!("{}", l.num_cols)),
            info_row("Created by:", short_str(&l.created_by, 60)),
        ]
        .spacing(2);

        let total: i64 = l.row_groups.iter().map(|r| r.total_bytes).sum();
        let mut strip_children: Vec<Element<'_, Message>> = Vec::new();
        if total > 0 {
            for (i, rg) in l.row_groups.iter().enumerate() {
                let p = (((rg.total_bytes.max(0) as f64 / total as f64) * 10_000.0).round() as u16).max(1);
                let color = palette(i);
                strip_children.push(
                    container(space())
                        .width(Length::FillPortion(p))
                        .height(Length::Fixed(16.0))
                        .style(move |_| container::Style {
                            background: Some(Background::Color(color)),
                            border: Border { radius: 2.0.into(), ..Border::default() },
                            ..container::Style::default()
                        })
                        .into(),
                );
            }
        }
        let strip = column![
            text("Row-group byte strip (proportional to total_byte_size):").size(12),
            row(strip_children).spacing(0).height(Length::Fixed(16.0)),
            text(format!("total {}", fmt_size(total))).font(MONO).size(11),
        ]
        .spacing(4);

        let s = self.schema_state();
        let mut schema_children: Vec<Element<'_, Message>> = vec![
            row![
                text("Column").width(Length::FillPortion(4)).font(BOLD),
                text("Physical").width(Length::FillPortion(3)).font(BOLD),
                text("Logical").width(Length::FillPortion(5)).font(BOLD),
            ]
            .spacing(8)
            .into(),
        ];
        if s {
            for c in &l.schema_cols {
                schema_children.push(
                    row![
                        text(c.name.clone()).font(MONO).size(11).width(Length::FillPortion(4)),
                        text(c.phys.clone()).font(MONO).size(11).width(Length::FillPortion(3)),
                        text(short_str(&c.logical, 60)).size(11).width(Length::FillPortion(5)),
                    ]
                    .spacing(8)
                    .into(),
                );
            }
        }
        let schema_section = column![
            section_button(s, format!("Schema ({} columns)", l.schema_cols.len()), Message::ToggleSchema),
            column(schema_children).spacing(2),
        ]
        .spacing(4);

        let kv_section = column![
            section_button(self.kv_open, format!("Key-value metadata ({})", l.kv.len()), Message::ToggleKv),
            if self.kv_open {
                column(
                    l.kv.iter()
                        .map(|(k, v)| {
                            row![
                                text(k.clone()).font(MONO).size(11),
                                text(short_str(v, 80)).size(11),
                            ]
                            .spacing(12)
                            .into()
                        })
                        .collect::<Vec<Element<'_, Message>>>(),
                )
                .spacing(2)
                .into()
            } else {
                Element::from(space())
            },
        ]
        .spacing(4);

        let mut rgba: Vec<Element<'_, Message>> = Vec::new();
        rgba.push(text(format!("Row groups ({})", l.row_groups.len())).size(15).font(BOLD).into());
        for rg in &l.row_groups {
            let open = self.expanded_rgs.contains(&rg.idx);
            let title = format!(
                "RG {} — {} rows — {} (compressed {})",
                rg.idx,
                rg.num_rows,
                fmt_size(rg.total_bytes),
                fmt_size(rg.compressed_bytes)
            );
            rgba.push(section_button(open, title, Message::ToggleRowGroup(rg.idx)));
            if open {
                let mut rgs: Vec<Element<'_, Message>> = vec![chunk_header()];
                for c in &rg.chunks {
                    rgs.push(chunk_row(c));
                }
                rgba.push(
                    scrollable(column(rgs).spacing(2))
                        .direction(scrollable::Direction::Horizontal(
                            scrollable::Scrollbar::new(),
                        ))
                        .style(grey_scrollable_style)
                        .into(),
                );
            }
        }

        let legend = text(
            "Legend: n=nulls, B=bloom, C=column index, O=offset index, D=dict page, I=index page.",
        )
        .size(11);

        let content = column![
            info,
            strip,
            schema_section,
            kv_section,
            column(rgba).spacing(4),
            legend,
        ]
        .spacing(10)
        .padding(8);

        scrollable(content)
            .style(grey_scrollable_style)
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
    }

    fn schema_state(&self) -> bool {
        self.schema_open
    }

    fn view(&self) -> Element<'_, Message> {
        let toolbar = container(self.view_toolbar()).width(Length::Fill);
        let body: Element<'_, Message> = if self.show_layout {
            self.view_layout()
        } else {
            self.view_data()
        };
        let content = column![toolbar, body].height(Length::Fill);

        if self.hovering {
            let overlay = container(text("Drop parquet file here").size(24))
                .center(Length::Fill)
                .width(Length::Fill)
                .height(Length::Fill)
                .style(|_| container::Style {
                    background: Some(Background::Color(Color::from_rgba(0.0, 0.0, 0.0, 0.35))),
                    ..container::Style::default()
                });
            stack(vec![content.into(), overlay.into()]).into()
        } else {
            content.into()
        }
    }

    fn title(&self) -> String {
        match &self.path {
            Some(path) => {
                let fname = Path::new(path).file_name().unwrap_or_default().to_string_lossy();
                format!(
                    "Parquet Viewer — {} — {} rows × {} cols",
                    fname, self.total_rows, self.columns.len()
                )
            }
            None => "Parquet Viewer".to_string(),
        }
    }

    fn subscription(&self) -> Subscription<Message> {
        let mut subscriptions = vec![
            iced::window::events().map(|(_id, event)| Message::WindowEvent(event)),
        ];

        // One-shot startup tick: registers the AppleEvent handler and applies
        // the scroll target left by the boot page load (widget operations only
        // apply after the first layout). Dropped as soon as it fires.
        if !self.started {
            subscriptions.push(
                iced::time::every(Duration::from_millis(250)).map(|_| Message::Started),
            );
        }

        // Event-driven; emits only when macOS delivers an `odoc`/`GURL` event.
        #[cfg(target_os = "macos")]
        subscriptions.push(macos_open::subscription());

        // One-shot auto-open dialog. No recurring timer: a periodic redraw
        // keeps the grid's GPU buffers alive (~92 MB on macOS).
        if !self.auto_open_done {
            let delay = if cfg!(target_os = "macos") { 2000 } else { 250 };
            subscriptions.push(
                iced::time::every(Duration::from_millis(delay)).map(|_| Message::AutoOpen),
            );
        }

        // While a column divider is held, follow the pointer globally; the
        // header handle only sees the initial press.
        if self.resizing.is_some() {
            subscriptions.push(iced::event::listen_with(|event, _status, _id| {
                match event {
                    iced::event::Event::Mouse(iced::mouse::Event::CursorMoved {
                        position,
                    }) => Some(Message::ResizeMove(position.x)),
                    iced::event::Event::Mouse(
                        iced::mouse::Event::ButtonReleased(iced::mouse::Button::Left),
                    ) => Some(Message::ResizeEnd),
                    _ => None,
                }
            }));
        }

        Subscription::batch(subscriptions)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::OpenDialog => Self::pick_file_task(),
            Message::FilePicked(Some(path)) => {
                self.auto_open_done = true;
                self.open_path(&path.to_string_lossy());
                self.take_scroll_task()
            }
            Message::FilePicked(None) => Task::none(),
            Message::WindowEvent(iced::window::Event::FileDropped(path)) => {
                self.hovering = false;
                self.auto_open_done = true;
                self.open_path(&path.to_string_lossy());
                self.take_scroll_task()
            }
            Message::WindowEvent(iced::window::Event::FileHovered(_)) => {
                self.hovering = true;
                Task::none()
            }
            Message::WindowEvent(iced::window::Event::FilesHoveredLeft) => {
                self.hovering = false;
                Task::none()
            }
            Message::WindowEvent(iced::window::Event::Resized(size)) => {
                let old_width = self.window_width;
                self.window_height = size.height;
                self.window_width = size.width;
                self.resizing = None;
                if old_width > 0.0 && !self.col_widths.is_empty() {
                    let n = self.col_widths.len();
                    let separators = n.saturating_sub(1) as f32;
                    let old_available = (old_width - separators).max(1.0);
                    let new_available = (size.width - separators).max(1.0);
                    let scale = new_available / old_available;
                    for w in &mut self.col_widths {
                        *w = (*w * scale).max(GRID_MIN_COL_W);
                    }
                }
                let rows = self.rows.len();
                if rows > 0 {
                    let first = self.grid_window.start.min(rows - 1);
                    let last = (first + self.grid_capacity()).min(rows);
                    self.grid_window = first..last;
                }
                Task::none()
            }
            Message::ResizeStart(col) => {
                if col + 1 < self.col_widths.len() {
                    self.resizing = Some(ResizeState {
                        col,
                        start_x: None,
                        left: self.col_widths[col],
                        right: self.col_widths[col + 1],
                    });
                }
                Task::none()
            }
            Message::ResizeMove(x) => {
                if let Some(mut r) = self.resizing {
                    match r.start_x {
                        None => {
                            r.start_x = Some(x);
                            self.resizing = Some(r);
                        }
                        Some(start) => {
                            let total = r.left + r.right;
                            let new_left = (r.left + (x - start))
                                .clamp(GRID_MIN_COL_W, total - GRID_MIN_COL_W);
                            if r.col + 1 < self.col_widths.len() {
                                self.col_widths[r.col] = new_left;
                                self.col_widths[r.col + 1] = total - new_left;
                            }
                            self.resizing = Some(r);
                        }
                    }
                }
                Task::none()
            }
            Message::ResizeEnd => {
                self.resizing = None;
                Task::none()
            }
            Message::WindowEvent(iced::window::Event::Unfocused) => {
                self.resizing = None;
                Task::none()
            }
            Message::WindowEvent(_) => Task::none(),
            Message::GridScrolled { x, y, viewport_h } => {
                let rows = self.rows.len();
                if rows > 0 {
                    let first = (y.max(0.0) / GRID_PITCH_Y).floor() as usize;
                    let visible = (viewport_h / GRID_PITCH_Y).ceil() as usize + 1;
                    let first = first.saturating_sub(GRID_OVERSCAN);
                    let last = (first + visible + 2 * GRID_OVERSCAN).min(rows);
                    self.grid_window = first..last;
                }
                // Header is outside the vertical scroller; mirror its
                // horizontal offset so the columns stay aligned.
                if (x - self.grid_offset_x).abs() > 0.5 {
                    self.grid_offset_x = x;
                    return iced::widget::operation::scroll_to(
                        HEADER_ID,
                        iced::widget::operation::AbsoluteOffset {
                            x: Some(x),
                            y: Some(0.0),
                        },
                    );
                }
                Task::none()
            }
            Message::HeaderScrolled { x } => {
                if (x - self.grid_offset_x).abs() > 0.5 {
                    self.grid_offset_x = x;
                    return iced::widget::operation::scroll_to(
                        GRID_ID,
                        iced::widget::operation::AbsoluteOffset {
                            x: Some(x),
                            y: None,
                        },
                    );
                }
                Task::none()
            }
            Message::Started => {
                self.started = true;

                #[cfg(target_os = "macos")]
                macos_open::install_apple_event_inner();

                // Consume the scroll target left by the boot page load.
                if self.scroll.is_some() {
                    return self.take_scroll_task();
                }
                Task::none()
            }
            Message::AutoOpen => {
                if !self.auto_open_done && self.path.is_none() {
                    self.auto_open_done = true;
                    return Self::pick_file_task();
                }
                Task::none()
            }
            Message::ExternalOpen(path) => {
                debug_log(&format!("ExternalOpen: {:?}", path));
                self.auto_open_done = true;
                self.open_path(&path);
                self.take_scroll_task()
            }
            Message::ToggleShowAll => {
                self.show_all = !self.show_all;
                self.update_total_pages();
                let target = if self.show_all { 0 } else { self.total_pages.saturating_sub(1) };
                self.load_page(target);
                self.take_scroll_task()
            }
            Message::PrevPage => {
                let p = self.current_page.saturating_sub(1);
                self.load_page(p);
                self.take_scroll_task()
            }
            Message::NextPage => {
                let p = self.current_page + 1;
                self.load_page(p);
                self.take_scroll_task()
            }
            Message::ShowData => {
                self.show_layout = false;
                Task::none()
            }
            Message::ShowLayout => {
                self.show_layout = true;
                Task::none()
            }
            Message::ToggleSchema => {
                self.schema_open = !self.schema_open;
                Task::none()
            }
            Message::ToggleKv => {
                self.kv_open = !self.kv_open;
                Task::none()
            }
            Message::ToggleRowGroup(idx) => {
                if !self.expanded_rgs.remove(&idx) {
                    self.expanded_rgs.insert(idx);
                }
                Task::none()
            }
            Message::YearPicked(y) => {
                self.sel_year = y;
                self.sel_day = self.sel_day.min(days_in_month(self.sel_year, self.sel_month));
                Task::none()
            }
            Message::MonthPicked(m) => {
                self.sel_month = m;
                self.sel_day = self.sel_day.min(days_in_month(self.sel_year, self.sel_month));
                Task::none()
            }
            Message::DayPicked(d) => {
                self.sel_day = d;
                Task::none()
            }
            Message::GoToDate => {
                let d = format!("{:04}-{:02}-{:02}", self.sel_year, self.sel_month, self.sel_day);
                self.goto_date(&d);
                self.take_scroll_task()
            }
        }
    }
}

fn info_row<'a>(label: &'static str, value: String) -> Element<'a, Message> {
    row![
        text(label).size(11).width(Length::Fixed(110.0)),
        text(value).font(MONO).size(11),
    ]
    .spacing(8)
    .into()
}

fn palette(i: usize) -> Color {
    const PALETTE: [Color; 4] = [
        Color::from_rgb(93.0 / 255.0, 165.0 / 255.0, 242.0 / 255.0),
        Color::from_rgb(142.0 / 255.0, 199.0 / 255.0, 123.0 / 255.0),
        Color::from_rgb(242.0 / 255.0, 178.0 / 255.0, 93.0 / 255.0),
        Color::from_rgb(200.0 / 255.0, 130.0 / 255.0, 220.0 / 255.0),
    ];
    PALETTE[i % PALETTE.len()]
}

fn section_button(open: bool, label: String, message: Message) -> Element<'static, Message> {
    button(text(format!("{} {}", if open { "v" } else { ">" }, label)).size(12))
        .style(button::text)
        .on_press(message)
        .into()
}

fn chunk_header<'a>() -> Element<'a, Message> {
    let w = |label: &'static str, px: f32| text(label).font(BOLD).size(11).width(Length::Fixed(px));
    row![
        w("Column", 170.0),
        w("Phys", 80.0),
        w("Codec", 80.0),
        w("Values", 70.0),
        w("Comp", 70.0),
        w("Uncomp", 80.0),
        w("DataOff", 90.0),
        w("Min", 100.0),
        w("Max", 100.0),
        w("Nulls / flags", 100.0),
        w("Encodings", 200.0),
    ]
    .spacing(6)
    .into()
}

fn chunk_row<'a>(c: &LayoutChunk) -> Element<'a, Message> {
    let mut flags = Vec::new();
    if c.has_bloom { flags.push("B"); }
    if c.has_col_index { flags.push("C"); }
    if c.has_offset_index { flags.push("O"); }
    if c.dict_offset.is_some() { flags.push("D"); }
    if c.index_offset.is_some() { flags.push("I"); }
    let nc = c.null_count.map(|n| n.to_string()).unwrap_or_else(|| "—".to_string());
    let w = |s: String, px: f32| text(s).font(MONO).size(11).width(Length::Fixed(px));

    row![
        w(short_str(&c.name, 28), 170.0),
        w(c.phys.clone(), 80.0),
        w(c.codec.clone(), 80.0),
        w(format!("{}", c.num_values), 70.0),
        w(fmt_size(c.compressed), 70.0),
        w(fmt_size(c.uncompressed), 80.0),
        w(format!("{}", c.data_offset), 90.0),
        w(short_str(&c.stat_min, 18), 100.0),
        w(short_str(&c.stat_max, 18), 100.0),
        w(format!("n={} {}", nc, flags.join("")), 100.0),
        w(short_str(&c.encodings, 24), 200.0),
    ]
    .spacing(6)
    .into()
}

fn main() -> iced::Result {
    // CLI file arg (terminal open, Parquet Opener helper droplet forwarding).
    // Warm AppleEvent opens while running arrive via the macos_open queue.
    let args: Vec<String> = std::env::args().collect();
    let mut initial_path: Option<String> = None;
    for arg in args.iter().skip(1) {
        if arg.starts_with('-') { continue; }
        let rp = resolve_path(&arg);
        if Path::new(&rp).is_file() { initial_path = Some(rp); break; }
    }

    let boot_path = initial_path.clone();
    iced::application(move || ParquetViewerApp::boot(boot_path.clone()), ParquetViewerApp::update, ParquetViewerApp::view)
        .title(ParquetViewerApp::title)
        .theme(|_: &ParquetViewerApp| Theme::Light)
        .settings(iced::Settings {
            default_font: APP_FONT,
            default_text_size: iced::Pixels(UI_FONT),
            ..Default::default()
        })
        .subscription(ParquetViewerApp::subscription)
        .window(iced::window::Settings {
            size: iced::Size::new(750.0, 950.0),
            min_size: Some(iced::Size::new(700.0, 500.0)),
            ..Default::default()
        })
        .run()
}
