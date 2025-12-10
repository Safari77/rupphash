use std::path::{PathBuf};
use std::fs;
use std::io::{self, Write};
use chrono::{DateTime, Utc};
use clap::Parser;
use jiff::{Timestamp};
use std::collections::{HashMap};

use crate::db::AppContext;
use crate::scanner::ScanConfig;
// Import the shared helper
use crate::state::get_bit_identical_counts;

mod rupphash;
mod mih;
mod db;
mod ui;
mod gui;
mod state;
mod scanner;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub path: PathBuf,
    pub size: u64,
    pub modified: DateTime<Utc>,
    pub phash: u64,
    pub resolution: Option<(u32, u32)>,
    pub content_hash: [u8; 32],
    pub orientation: u8, // Added: EXIF orientation (1-8)
    pub dev_inode: Option<(u64, u64)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupStatus {
    AllIdentical,
    SomeIdentical,
    None,
}

#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub max_dist: u32,
    pub status: GroupStatus,
}

// --- Helper: Relative Time ---
pub fn format_relative_time(ts: Timestamp) -> String {
    let now = Timestamp::now();
    let zoned_ts = ts.to_zoned(jiff::tz::TimeZone::UTC);
    let raw_span = now.since(ts).unwrap_or_default();
    let total_secs = raw_span.total(jiff::Unit::Second).unwrap_or(0.0).abs();

    if total_secs < 60.0 {
        if total_secs < 0.001 { return "0s".to_string(); }
        return format!("{:.3}s", total_secs);
    }

    let span = raw_span.round(jiff::SpanRound::new()
            .largest(jiff::Unit::Year)
            .smallest(jiff::Unit::Second)
            .relative(&zoned_ts)
        ).unwrap_or_default();

    let mut parts = Vec::new();
    let y = span.get_years().abs();
    let mo = span.get_months().abs();
    let w = span.get_weeks().abs();
    let d = span.get_days().abs();
    let h = span.get_hours().abs();
    let m = span.get_minutes().abs();
    let s = span.get_seconds().abs();

    if y > 0 { parts.push(format!("{}y", y)); }
    if mo > 0 { parts.push(format!("{}mo", mo)); }
    if w > 0 { parts.push(format!("{}w", w)); }
    if d > 0 { parts.push(format!("{}d", d)); }
    if h > 0 { parts.push(format!("{}h", h)); }
    if m > 0 { parts.push(format!("{}m", m)); }
    if s > 0 { parts.push(format!("{}s", s)); }

    if parts.is_empty() { return "0s".to_string(); }
    parts.into_iter().take(3).collect::<Vec<_>>().join(" ")
}

// --- Analysis Logic ---
// Moved to phdupes.rs if needed, but keeping existing structure
pub fn analyze_group(
    files: &mut Vec<FileMetadata>,
    group_by: &str,
    ext_priorities: &HashMap<String, usize>
) -> GroupInfo {
    if files.is_empty() {
        return GroupInfo { max_dist: 0, status: GroupStatus::None };
    }

    let mut counts = HashMap::new();
    for f in files.iter() {
        *counts.entry(f.content_hash).or_insert(0) += 1;
    }

    let (mut duplicates, mut unique): (Vec<FileMetadata>, Vec<FileMetadata>) = files.drain(..)
        .partition(|f| *counts.get(&f.content_hash).unwrap_or(&0) > 1);

    let sorter = |a: &FileMetadata, b: &FileMetadata| {
        if group_by == "date" {
            a.modified.cmp(&b.modified)
        } else {
            let ext_a = a.path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
            let ext_b = b.path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();

            let prio_a = ext_priorities.get(&ext_a).unwrap_or(&usize::MAX);
            let prio_b = ext_priorities.get(&ext_b).unwrap_or(&usize::MAX);

            match prio_a.cmp(prio_b) {
                std::cmp::Ordering::Equal => b.size.cmp(&a.size),
                other => other,
            }
        }
    };

    duplicates.sort_by(sorter);
    unique.sort_by(sorter);

    files.append(&mut duplicates);
    files.append(&mut unique);

    let pivot_phash = files[0].phash;
    let max_d = files.iter().map(|f| (f.phash ^ pivot_phash).count_ones()).max().unwrap_or(0);

    let has_duplicates = !counts.values().all(|&c| c == 1);
    let all_identical = counts.len() == 1;

    let status = if all_identical {
        GroupStatus::AllIdentical
    } else if has_duplicates {
        GroupStatus::SomeIdentical
    } else {
        GroupStatus::None
    };

    GroupInfo {
        max_dist: max_d,
        status,
    }
}

// --- CLI Definition ---

#[derive(Parser, Debug)]
#[command(author, version, about = "Finds visually similar images.", long_about = None)]
struct Cli {
    #[arg(required = true)]
    paths: Vec<String>,

    #[arg(long)]
    rehash: bool,
    #[arg(long)]
    rehash_only: bool,
    #[arg(long, default_value_t = 5)]
    similarity: u32,

    /// Sort order: name, date, date-desc, size, size-desc, random
    #[arg(long, default_value = "name")]
    sort: String,

    // Legacy alias for --sort
    #[arg(long, default_value = "size", hide = true)]
    group_by: String,

    #[arg(long)]
    use_tui: bool,

    #[arg(long)]
    use_gui: bool,

    #[arg(long)]
    delete: bool,

    #[arg(long)]
    relative_times: bool,

    #[arg(long)]
    use_trash: bool,

    /// View mode: browse images without similarity checking
    #[arg(long)]
    view: bool,

    /// Shuffle images randomly (implies --view)
    #[arg(long)]
    shuffle: bool,

    /// Slideshow mode with interval in seconds (implies --view --use-gui)
    #[arg(long, value_name = "SECONDS")]
    slideshow: Option<f32>,

    /// Directory to move marked files to
    #[arg(long, value_name = "DIR")]
    move_marked: Option<PathBuf>,
}

impl Cli {
    fn validate(&self) -> Result<(), String> {
        if self.similarity > crate::mih::MAX_SIMILARITY {
            return Err(format!("Similarity must be 0-{}. Got {}.", crate::mih::MAX_SIMILARITY, self.similarity));
        }

        let valid_sorts = ["name", "date", "date-desc", "size", "size-desc", "random"];
        let sort_lower = self.sort.to_lowercase();
        if !valid_sorts.contains(&sort_lower.as_str()) {
            return Err(format!("Invalid sort '{}'. Use one of: {}", self.sort, valid_sorts.join(", ")));
        }

        if self.use_tui && self.use_gui {
             return Err("Cannot use both --use-tui and --use-gui".to_string());
        }

        if let Some(ref dir) = self.move_marked {
            if !dir.exists() {
                return Err(format!("Move target directory does not exist: {:?}", dir));
            }
            if !dir.is_dir() {
                return Err(format!("Move target is not a directory: {:?}", dir));
            }
        }

        if let Some(secs) = self.slideshow {
            if secs <= 0.0 {
                return Err("Slideshow interval must be positive".to_string());
            }
        }

        Ok(())
    }

    /// Get effective sort mode (handles legacy group_by)
    fn effective_sort(&self) -> String {
        // If --sort was explicitly set to something other than default, use it
        // Otherwise fall back to group_by for backwards compatibility
        if self.sort != "size" {
            self.sort.to_lowercase()
        } else if self.group_by != "size" {
            // Legacy: map old group_by values
            match self.group_by.to_lowercase().as_str() {
                "date" => "date".to_string(),
                _ => "size".to_string(),
            }
        } else {
            "size".to_string()
        }
    }

    /// Check if we're in view mode (explicit or implied)
    fn is_view_mode(&self) -> bool {
        self.view || self.shuffle || self.slideshow.is_some()
    }
}

// --- CLI Helpers ---
// Removed local `get_bit_identical_counts` in favor of state::get_bit_identical_counts

fn format_size(bytes: u64) -> String {
    if bytes < 1024 { return format!("{} B", bytes); }
    let kb = bytes as f64 / 1024.0;
    if kb < 1024.0 { return format!("{:.1} KB", kb); }
    let mb = kb / 1024.0;
    format!("{:.1} MB", mb)
}

fn run_interactive_cli_delete(
    groups: Vec<Vec<FileMetadata>>,
    group_infos: Vec<GroupInfo>,
    show_relative_times: bool,
    use_trash: bool
) {
    let mut input_buf = String::new();
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for (g_idx, group) in groups.iter().enumerate() {
        if group.len() < 2 { continue; }
        let info = &group_infos[g_idx];
        let green = "\x1b[32m";
        let reset = "\x1b[0m";

        println!("\n========================================================");
        match info.status {
            GroupStatus::AllIdentical => println!("Group {} - {}Bit-identical{}", g_idx + 1, green, reset),
            GroupStatus::SomeIdentical => println!("Group {} - {}Some files Bit-identical{}", g_idx + 1, green, reset),
            GroupStatus::None => println!("Group {}/{} (Max Dist: {})", g_idx + 1, groups.len(), info.max_dist),
        }
        println!("========================================================");

        // REFACTORED: Use shared helper
        let counts = get_bit_identical_counts(group);

        for (i, file) in group.iter().enumerate() {
            let time_str = if show_relative_times {
                let ts = Timestamp::from_second(file.modified.timestamp()).unwrap()
                    .checked_add(jiff::SignedDuration::from_nanos(file.modified.timestamp_subsec_nanos() as i64)).unwrap();
                format_relative_time(ts)
            } else {
                file.modified.format("%Y-%m-%d %H:%M:%S").to_string()
            };
            let res_str = file.resolution.map(|(w,h)| format!("{}x{}", w, h)).unwrap_or("???x???".to_string());
            let is_identical = *counts.get(&file.content_hash).unwrap_or(&0) > 1;
            let (color_start, color_end, marker) = if is_identical { (green, reset, "*") } else { ("", "", " ") };

            println!("{}[{}] {} {} | {} | {} | {}{}", color_start, i + 1, marker, time_str, format_size(file.size), res_str, file.path.display(), color_end);
        }

        let action_verb = if use_trash { "TRASH" } else { "PERMANENTLY delete" };
        print!("\nEnter numbers to {} (e.g. '1 3'), or ENTER to skip: ", action_verb);
        stdout.flush().ok();

        input_buf.clear();
        if stdin.read_line(&mut input_buf).is_ok() {
            let line = input_buf.trim();
            if line.is_empty() { continue; }
            let indices: Vec<usize> = line.split_whitespace()
                .filter_map(|s| s.parse::<usize>().ok())
                .filter(|&idx| idx >= 1 && idx <= group.len())
                .map(|idx| idx - 1)
                .collect();

            if indices.is_empty() { println!("No valid selections."); continue; }

            for &idx in &indices {
                let file = &group[idx];
                print!("{} {:?} ... ", if use_trash { "Trashing" } else { "Deleting" }, file.path.file_name().unwrap_or_default());
                let res = if use_trash {
                    trash::delete(&file.path).map_err(|e| io::Error::other(e.to_string()))
                } else {
                    fs::remove_file(&file.path)
                };
                match res { Ok(_) => println!("OK"), Err(e) => println!("FAILED ({})", e), }
            }
        }
    }
    println!("\nDone.");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    if let Err(e) = args.validate() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    image_extras::register();

    let sort_order = args.effective_sort();
    let is_view_mode = args.is_view_mode();

    // View mode uses GUI by default unless --use-tui specified
    let use_gui = args.use_gui || (is_view_mode && !args.use_tui);

    // View mode with GUI
    if is_view_mode && use_gui {
        println!("Launching image viewer...");
        let app = gui::GuiApp::new_view_mode(
            args.paths.clone(),
            sort_order,
            args.relative_times,
            args.use_trash,
            args.move_marked.clone(),
            args.slideshow,
        );
        if let Err(e) = app.run() {
            eprintln!("GUI Error: {}", e);
        }
        return Ok(());
    }

    // Duplicate detection modes require AppContext
    let ctx = AppContext::new()?;

    let scan_config = ScanConfig {
        paths: args.paths.clone(),
        rehash: args.rehash,
        similarity: args.similarity,
        group_by: sort_order.clone(),
        extensions: ctx.grouping_config.extensions.clone(),
        ignore_same_stem: ctx.grouping_config.ignore_same_stem,
    };

    if args.rehash_only {
        let _ = scanner::scan_and_group(&scan_config, &ctx, None);
        return Ok(());
    }

    // For GUI mode (duplicate detection), let the GUI handle scanning with progress display
    if use_gui {
        let ext_priorities: HashMap<String, usize> = ctx.grouping_config.extensions.iter()
            .enumerate()
            .map(|(i, e)| (e.to_lowercase(), i))
            .collect();

        println!("Launching GUI...");
        let app = gui::GuiApp::new(
            ctx,
            scan_config,
            args.relative_times,
            args.use_trash,
            sort_order,
            ext_priorities
        ).with_move_target(args.move_marked.clone());

        if let Err(e) = app.run() {
             eprintln!("GUI Error: {}", e);
        }
        return Ok(());
    }

    // For non-GUI modes, scan first then display results
    let (final_groups, final_infos) = scanner::scan_and_group(&scan_config, &ctx, None);
    println!("Found {} duplicate groups.", final_groups.len());

    if args.use_tui {
        let ext_priorities: HashMap<String, usize> = ctx.grouping_config.extensions.iter()
            .enumerate()
            .map(|(i, e)| (e.to_lowercase(), i))
            .collect();

        let mut state = state::AppState::new(
            final_groups,
            final_infos,
            args.relative_times,
            args.use_trash,
            sort_order,
            ext_priorities
        );
        state.move_target = args.move_marked.clone();

        println!("Launching TUI...");
        let mut app = ui::TuiApp::new(state);
        app.run()?;

    } else if args.delete {
        run_interactive_cli_delete(final_groups, final_infos, args.relative_times, args.use_trash);
    } else {
        let green = "\x1b[32m";
        let reset = "\x1b[0m";
        for (i, group) in final_groups.iter().enumerate() {
            let info = &final_infos[i];
            match info.status {
                 GroupStatus::AllIdentical => println!("\n--- Group {} - {}Bit-identical{} ---", i + 1, green, reset),
                 GroupStatus::SomeIdentical => println!("\n--- Group {} - {}Some files Bit-identical{} ---", i + 1, green, reset),
                 GroupStatus::None => println!("\n--- Group {} (Max Dist: {}) ---", i + 1, info.max_dist),
            }

            // REFACTORED: Use shared helper
            let counts = get_bit_identical_counts(group);

            for file in group {
                let time_str = if args.relative_times {
                     let ts = Timestamp::from_second(file.modified.timestamp()).unwrap()
                        .checked_add(jiff::SignedDuration::from_nanos(file.modified.timestamp_subsec_nanos() as i64)).unwrap();
                     format_relative_time(ts)
                } else {
                     file.modified.format("%Y-%m-%d %H:%M:%S.%f").to_string()
                };
                let res_str = file.resolution.map(|(w,h)| format!("{}x{}", w, h)).unwrap_or("?".to_string());
                let is_identical = *counts.get(&file.content_hash).unwrap_or(&0) > 1;
                let (color_start, color_end, marker) = if is_identical { (green, reset, "*") } else { ("", "", " ") };
                println!("  {}[{}] {} | {} | {} | {}{}", color_start, marker, time_str, format_size(file.size), res_str, file.path.display(), color_end);
            }
        }
    }
    Ok(())
}
