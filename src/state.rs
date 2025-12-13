use std::path::{PathBuf, Path};
use std::collections::{HashMap, HashSet};
use std::fs;
use crate::{FileMetadata, GroupInfo};
use crate::scanner::{analyze_group, sort_files};

#[derive(Debug, Clone, PartialEq)]
pub enum InputIntent {
    NextItem,
    PrevItem,
    NextGroup,
    PrevGroup,
    PageDown,
    PageUp,
    Home,
    End,
    ToggleMark,
    ConfirmDelete,
    ExecuteDelete,
    DeleteImmediate,      // Delete current file without marking (for view mode)
    ConfirmDeleteImmediate,
    MoveMarked,           // Move marked files to target directory
    ConfirmMoveMarked,
    Cancel,
    Quit,
    ToggleRelativeTime,
    CycleViewMode,
    CycleZoom,
    StartRename,
    SubmitRename(String),
    ReloadList,
    ToggleZoomRelative,
    TogglePathVisibility,
    ToggleSlideshow,      // Pause/resume slideshow
    ToggleFullscreen,
    RotateCW,
    ShowSortSelection,
    ChangeSortOrder(String),
}

#[derive(Debug, Clone)]
pub struct RenameState {
    pub group_idx: usize,
    pub file_idx: usize,
    pub original_path: PathBuf,
}

// --- Shared Helpers ---

/// Formats a path to show only the last `depth + 1` components.
pub fn format_path_depth(path: &Path, depth: usize) -> String {
    let components: Vec<_> = path.components().collect();
    if components.is_empty() { return "".to_string(); }

    let take = depth + 1;
    let len = components.len();

    let start = len.saturating_sub(take);

    let mut out = PathBuf::new();
    for c in &components[start..] {
        out.push(c);
    }

    out.to_string_lossy().to_string()
}

/// Returns a map of content_hash -> count for a group of files.
/// Used to detect and highlight bit-identical files in UIs.
pub fn get_bit_identical_counts(group: &[FileMetadata]) -> HashMap<[u8; 32], usize> {
    let mut counts = HashMap::new();
    for f in group {
        *counts.entry(f.content_hash).or_insert(0) += 1;
    }
    counts
}

pub fn get_content_identical_counts(files: &[FileMetadata]) -> HashMap<[u8; 32], usize> {
    let mut counts = HashMap::new();
    for f in files {
        if let Some(ph) = f.pixel_hash {
            *counts.entry(ph).or_insert(0) += 1;
        }
    }
    counts
}

pub fn get_content_subgroups(group: &[FileMetadata]) -> HashMap<[u8; 32], usize> {
    let mut counts = HashMap::new();
    for f in group {
        if let Some(ph) = f.pixel_hash {
            *counts.entry(ph).or_insert(0) += 1;
        }
    }

    let mut ids = HashMap::new();
    let mut next_id = 1;

    // Assign IDs in order of appearance in the list to keep UI stable
    for f in group {
        if let Some(ph) = f.pixel_hash {
            // Only assign an ID if this hash appears more than once (is a duplicate)
            if *counts.get(&ph).unwrap_or(&0) > 1 {
                if !ids.contains_key(&ph) {
                    ids.insert(ph, next_id);
                    next_id += 1;
                }
            }
        }
    }
    ids
}

// --- AppState ---

pub struct AppState {
    pub groups: Vec<Vec<FileMetadata>>,
    pub group_infos: Vec<GroupInfo>,
    pub current_group_idx: usize,
    pub current_file_idx: usize,
    pub marked_for_deletion: Vec<PathBuf>,
    pub renaming: Option<RenameState>,
    pub show_relative_times: bool,
    pub use_trash: bool,
    pub group_by: String,
    pub ext_priorities: HashMap<String, usize>,
    pub status_message: Option<(String, bool)>,
    pub show_confirmation: bool,
    pub show_move_confirmation: bool,
    pub show_delete_immediate_confirmation: bool,
    pub show_sort_selection: bool,
    pub error_popup: Option<String>,
    pub exit_requested: bool,
    pub selection_changed: bool,
    pub is_loading: bool,
    pub last_file_count: usize,
    pub zoom_relative: bool,
    pub path_display_depth: usize,

    // View mode features
    pub view_mode: bool,
    pub move_target: Option<PathBuf>,
    pub slideshow_interval: Option<f32>,
    pub slideshow_paused: bool,
    pub is_fullscreen: bool,
    pub manual_rotation: u8,
    pub use_pdqhash: bool,
}

impl AppState {
    pub fn new(
        groups: Vec<Vec<FileMetadata>>,
        group_infos: Vec<GroupInfo>,
        show_relative_times: bool,
        use_trash: bool,
        group_by: String,
        ext_priorities: HashMap<String, usize>,
        use_pdqhash: bool,
    ) -> Self {
        let count = groups.iter().map(|g| g.len()).sum();
        Self {
            groups,
            group_infos,
            current_group_idx: 0,
            current_file_idx: 0,
            marked_for_deletion: Vec::new(),
            renaming: None,
            show_relative_times,
            use_trash,
            group_by,
            ext_priorities,
            status_message: None,
            show_confirmation: false,
            show_move_confirmation: false,
            show_delete_immediate_confirmation: false,
            show_sort_selection: false,
            error_popup: None,
            exit_requested: false,
            selection_changed: true,
            is_loading: false,
            last_file_count: count,
            zoom_relative: false,
            path_display_depth: 0,
            view_mode: false,
            move_target: None,
            slideshow_interval: None,
            slideshow_paused: false,
            is_fullscreen: false,
            manual_rotation: 0,
            use_pdqhash,
        }
    }

    pub fn handle_input(&mut self, intent: InputIntent) {
        self.selection_changed = false;

        if self.error_popup.is_some() {
            self.error_popup = None;
            return;
        }
        // Handle sort selection modal
        if self.show_sort_selection {
            match intent {
                InputIntent::ChangeSortOrder(sort) => {
                    self.show_sort_selection = false;
                    self.perform_sort(sort);
                },
                InputIntent::Cancel | InputIntent::Quit => {
                    self.show_sort_selection = false;
                }
                _ => {}
            }
            return;
        }

        // Handle delete confirmation modal
        if self.show_confirmation {
            match intent {
                InputIntent::ConfirmDelete => {
                    self.show_confirmation = false;
                    self.perform_deletion();
                },
                InputIntent::Cancel | InputIntent::Quit => {
                    self.show_confirmation = false;
                }
                _ => {}
            }
            return;
        }

        // Handle move confirmation modal
        if self.show_move_confirmation {
            match intent {
                InputIntent::ConfirmMoveMarked => {
                    self.show_move_confirmation = false;
                    self.perform_move_marked();
                },
                InputIntent::Cancel | InputIntent::Quit => {
                    self.show_move_confirmation = false;
                }
                _ => {}
            }
            return;
        }

        // Handle delete-immediate confirmation modal
        if self.show_delete_immediate_confirmation {
            match intent {
                InputIntent::ConfirmDeleteImmediate => {
                    self.show_delete_immediate_confirmation = false;
                    self.perform_delete_immediate();
                },
                InputIntent::Cancel | InputIntent::Quit => {
                    self.show_delete_immediate_confirmation = false;
                }
                _ => {}
            }
            return;
        }

        if self.renaming.is_some() {
            match intent {
                InputIntent::SubmitRename(new_name) => self.perform_rename(new_name),
                InputIntent::Cancel => self.renaming = None,
                _ => {}
            }
            return;
        }

        match intent {
            InputIntent::Quit => self.exit_requested = true,
            InputIntent::NextItem => { self.next_item(); self.selection_changed = true; },
            InputIntent::PrevItem => { self.prev_item(); self.selection_changed = true; },
            InputIntent::NextGroup => { self.next_group(); self.selection_changed = true; },
            InputIntent::PrevGroup => { self.prev_group(); self.selection_changed = true; },
            InputIntent::PageDown => { self.move_page(true, 15); self.selection_changed = true; },
            InputIntent::PageUp => { self.move_page(false, 15); self.selection_changed = true; },
            InputIntent::Home => { self.go_home(); self.selection_changed = true; },
            InputIntent::End => { self.go_end(); self.selection_changed = true; },
            InputIntent::ToggleMark => self.toggle_delete(),
            InputIntent::ExecuteDelete => {
                if !self.marked_for_deletion.is_empty() {
                    self.show_confirmation = true;
                } else if self.get_current_image_path().is_some() {
                    // If nothing marked, delete current file
                    self.show_delete_immediate_confirmation = true;
                } else {
                    self.set_status("No files to delete.".to_string(), false);
                }
            },
            InputIntent::DeleteImmediate => {
                if self.get_current_image_path().is_some() {
                    self.show_delete_immediate_confirmation = true;
                }
            },
            InputIntent::ConfirmDeleteImmediate => {},
            InputIntent::MoveMarked => {
                if self.move_target.is_none() {
                    self.set_status("No move target set (use --move-marked)".to_string(), true);
                } else if self.marked_for_deletion.is_empty() {
                    self.set_status("No files marked.".to_string(), false);
                } else {
                    self.show_move_confirmation = true;
                }
            },
            InputIntent::ConfirmMoveMarked => {},
            InputIntent::ToggleRelativeTime => {
                self.show_relative_times = !self.show_relative_times;
                self.selection_changed = true;
            },
            InputIntent::ConfirmDelete => {},
            InputIntent::Cancel => { self.status_message = None; },
            InputIntent::CycleViewMode | InputIntent::CycleZoom => {},
            InputIntent::StartRename => {
                if let Some(path) = self.get_current_image_path().cloned() {
                    self.renaming = Some(RenameState {
                        group_idx: self.current_group_idx,
                        file_idx: self.current_file_idx,
                        original_path: path
                    });
                }
            },
            InputIntent::SubmitRename(_) => {},
            InputIntent::ReloadList => { self.is_loading = true; },
            InputIntent::ToggleZoomRelative => {
                self.zoom_relative = !self.zoom_relative;
                self.selection_changed = true;
            },
            InputIntent::TogglePathVisibility => {
                if let Some(path) = self.get_current_image_path() {
                    let total_components = path.components().count();
                    if self.path_display_depth + 1 >= total_components {
                        self.path_display_depth = 0;
                    } else {
                        self.path_display_depth += 1;
                    }
                    self.selection_changed = true;
                }
            },
            InputIntent::ToggleSlideshow => {
                self.slideshow_paused = !self.slideshow_paused;
                let status = if self.slideshow_paused { "Slideshow paused" } else { "Slideshow resumed" };
                self.set_status(status.to_string(), false);
            },
            InputIntent::ToggleFullscreen => {
                self.is_fullscreen = !self.is_fullscreen;
            },
            InputIntent::RotateCW => {
                self.manual_rotation = (self.manual_rotation + 1) % 4;
            },
            InputIntent::ShowSortSelection => {
                self.show_sort_selection = true;
            },
            InputIntent::ChangeSortOrder(_) => {},
        }
    }

    fn set_status(&mut self, msg: String, is_error: bool) {
        self.status_message = Some((msg, is_error));
    }

    pub fn get_current_image_path(&self) -> Option<&PathBuf> {
        if self.groups.is_empty() { return None; }
        let group = &self.groups[self.current_group_idx];
        if self.current_file_idx < group.len() {
            Some(&group[self.current_file_idx].path)
        } else {
            None
        }
    }

    fn perform_rename(&mut self, new_name: String) {
        if let Some(rename_state) = self.renaming.take() {
            let parent = rename_state.original_path.parent().unwrap_or(std::path::Path::new("."));
            let new_path = parent.join(&new_name);

            if new_path.exists() {
                 self.error_popup = Some(format!("Error: Destination already exists:\n{:?}", new_path));
                 return;
            }

            match fs::rename(&rename_state.original_path, &new_path) {
                Ok(_) => {
                     if let Some(group) = self.groups.get_mut(rename_state.group_idx)
                         && let Some(file) = group.get_mut(rename_state.file_idx) {
                             file.path = new_path;
                             self.set_status(format!("Renamed to '{}'", new_name), false);
                         }
                     self.selection_changed = true;
                },
                Err(e) => {
                     self.error_popup = Some(format!("Failed to rename:\n{}", e));
                }
            }
        }
    }

    fn perform_sort(&mut self, sort_order: String) {
        // Capture current file path to preserve selection
        let current_path = self.get_current_image_path().cloned();

        for group in &mut self.groups {
            sort_files(group, &sort_order);
        }

        // Restore selection
        if let Some(path) = current_path {
            if let Some(group) = self.groups.get(self.current_group_idx) {
                if let Some(new_idx) = group.iter().position(|f| f.path == path) {
                    self.current_file_idx = new_idx;
                } else {
                     // Fallback if file not found (unlikely unless list changed concurrently)
                     self.current_file_idx = 0;
                }
            }
        } else {
            self.current_file_idx = 0;
        }

        self.set_status(format!("Sorted by: {}", sort_order), false);
        self.selection_changed = true;
    }

    pub fn next_item(&mut self) {
        if self.groups.is_empty() { return; }
        self.manual_rotation = 0; // Reset rotation
        let group_len = self.groups[self.current_group_idx].len();
        if self.current_file_idx + 1 < group_len { self.current_file_idx += 1; }
        else if self.current_group_idx + 1 < self.groups.len() { self.current_group_idx += 1; self.current_file_idx = 0; }
    }
    fn prev_item(&mut self) {
        if self.groups.is_empty() { return; }
        self.manual_rotation = 0; // Reset rotation
        if self.current_file_idx > 0 { self.current_file_idx -= 1; }
        else if self.current_group_idx > 0 { self.current_group_idx -= 1; self.current_file_idx = self.groups[self.current_group_idx].len() - 1; }
    }
    fn next_group(&mut self) {
        if self.groups.is_empty() { return; }
        self.manual_rotation = 0;
        self.current_group_idx = (self.current_group_idx + 1) % self.groups.len();
        self.current_file_idx = 0;
    }
    fn prev_group(&mut self) {
        if self.groups.is_empty() { return; }
        self.manual_rotation = 0;
        if self.current_group_idx == 0 { self.current_group_idx = self.groups.len() - 1; } else { self.current_group_idx -= 1; }
        self.current_file_idx = 0;
    }
    fn go_home(&mut self) { if !self.groups.is_empty() { self.current_group_idx = 0; self.current_file_idx = 0; self.manual_rotation = 0; } }
    fn go_end(&mut self) { if !self.groups.is_empty() { self.current_group_idx = self.groups.len() - 1; self.manual_rotation = 0; if let Some(g) = self.groups.last() { self.current_file_idx = g.len().saturating_sub(1); } } }

    pub fn move_page(&mut self, down: bool, view_size: usize) {
        if self.groups.is_empty() { return; }
        self.manual_rotation = 0;
        let mut current_abs = 0;
        for i in 0..self.current_group_idx { current_abs += 1 + self.groups[i].len(); }
        current_abs += 1 + self.current_file_idx;
        let total_rows: usize = self.groups.iter().map(|g| 1 + g.len()).sum();
        let scroll_amount = view_size.max(1);
        let target_abs = if down { current_abs.saturating_add(scroll_amount).min(total_rows - 1) } else { current_abs.saturating_sub(scroll_amount) };
        let mut accum = 0;
        for (g_idx, group) in self.groups.iter().enumerate() {
            let g_len = 1 + group.len();
            if target_abs < accum + g_len {
                let offset = target_abs - accum;
                if offset == 0 {
                    if down { self.current_group_idx = g_idx; self.current_file_idx = 0; }
                    else if g_idx > 0 { self.current_group_idx = g_idx - 1; self.current_file_idx = self.groups[g_idx - 1].len().saturating_sub(1); }
                    else { self.current_group_idx = 0; self.current_file_idx = 0; }
                } else { self.current_group_idx = g_idx; self.current_file_idx = offset - 1; }
                return;
            }
            accum += g_len;
        }
    }

    fn toggle_delete(&mut self) {
        if let Some(path) = self.get_current_image_path().cloned() {
            if self.marked_for_deletion.contains(&path) { self.marked_for_deletion.retain(|p| p != &path); } else { self.marked_for_deletion.push(path); }
        }
    }

    fn perform_deletion(&mut self) {
        if self.marked_for_deletion.is_empty() { return; }
        let mut success_count = 0;
        let mut failed_paths = HashSet::new();
        let deleted_paths = self.marked_for_deletion.clone();
        let mut error_details = Vec::new();

        for path in &deleted_paths {
            let res = if self.use_trash { trash::delete(path).map_err(|e| e.to_string()) } else { fs::remove_file(path).map_err(|e| e.to_string()) };
            match res {
                Ok(_) => success_count += 1,
                Err(e) => {
                    error_details.push(format!("• {:?}: {}", path.file_name().unwrap_or_default(), e));
                    failed_paths.insert(path.clone());
                },
            }
        }
        self.marked_for_deletion.retain(|p| failed_paths.contains(p));
        if success_count > 0 {
            for group in &mut self.groups { group.retain(|f| !deleted_paths.contains(&f.path) || failed_paths.contains(&f.path)); }
            let mut i = 0;
            while i < self.groups.len() {
                if self.groups[i].is_empty() { self.groups.remove(i); self.group_infos.remove(i); if self.current_group_idx >= i && self.current_group_idx > 0 { self.current_group_idx -= 1; } }
                else { self.group_infos[i] = analyze_group(&mut self.groups[i], &self.group_by, &self.ext_priorities, self.use_pdqhash); i += 1; }
            }
            if self.groups.is_empty() { self.current_group_idx = 0; self.current_file_idx = 0; }
            else {
                if self.current_group_idx >= self.groups.len() { self.current_group_idx = self.groups.len() - 1; }
                if self.current_file_idx >= self.groups[self.current_group_idx].len() { self.current_file_idx = self.groups[self.current_group_idx].len() - 1; }
            }
            self.selection_changed = true;
        }
        if failed_paths.is_empty() {
            let action = if self.use_trash { "trashed" } else { "permanently deleted" };
            self.set_status(format!("Successfully {} {} files.", action, success_count), false);
        } else {
            let mut full_msg = format!("Failed to delete {} files:\n\n", failed_paths.len());
            full_msg.push_str(&error_details.into_iter().take(5).collect::<Vec<_>>().join("\n"));
            if failed_paths.len() > 5 { full_msg.push_str("\n...and others."); }
            full_msg.push_str("\n\n(Press any key to dismiss)");
            self.error_popup = Some(full_msg);
        }
    }

    fn perform_delete_immediate(&mut self) {
        let Some(path) = self.get_current_image_path().cloned() else { return };

        let res = if self.use_trash {
            trash::delete(&path).map_err(|e| e.to_string())
        } else {
            fs::remove_file(&path).map_err(|e| e.to_string())
        };

        match res {
            Ok(_) => {
                let filename = path.file_name().unwrap_or_default().to_string_lossy().to_string();

                // Remove from current group
                if let Some(group) = self.groups.get_mut(self.current_group_idx) {
                    group.retain(|f| f.path != path);
                }

                // Clean up empty groups
                if self.groups.get(self.current_group_idx).map(|g| g.is_empty()).unwrap_or(false) {
                    self.groups.remove(self.current_group_idx);
                    self.group_infos.remove(self.current_group_idx);
                }

                // Adjust indices
                if self.groups.is_empty() {
                    self.current_group_idx = 0;
                    self.current_file_idx = 0;
                } else {
                    if self.current_group_idx >= self.groups.len() {
                        self.current_group_idx = self.groups.len() - 1;
                    }
                    if self.current_file_idx >= self.groups[self.current_group_idx].len() {
                        self.current_file_idx = self.groups[self.current_group_idx].len().saturating_sub(1);
                    }
                }

                // Also remove from marked list if it was there
                self.marked_for_deletion.retain(|p| p != &path);

                let action = if self.use_trash { "Trashed" } else { "Deleted" };
                self.set_status(format!("{}: {}", action, filename), false);
                self.selection_changed = true;
            },
            Err(e) => {
                self.error_popup = Some(format!("Failed to delete:\n{}", e));
            }
        }
    }

    fn perform_move_marked(&mut self) {
        let Some(ref target_dir) = self.move_target.clone() else {
            self.set_status("No move target set".to_string(), true);
            return;
        };

        if self.marked_for_deletion.is_empty() { return; }

        let mut success_count = 0;
        let mut failed_paths = HashSet::new();
        let paths_to_move = self.marked_for_deletion.clone();
        let mut error_details = Vec::new();

        for path in &paths_to_move {
            let filename = path.file_name().unwrap_or_default();
            let dest = target_dir.join(filename);

            // Check if destination exists
            if dest.exists() {
                error_details.push(format!("• {:?}: destination exists", filename));
                failed_paths.insert(path.clone());
                continue;
            }

            match fs::rename(path, &dest) {
                Ok(_) => {
                    success_count += 1;
                },
                Err(_) => {
                    // Try copy + delete if rename fails (cross-device)
                    match fs::copy(path, &dest).and_then(|_| fs::remove_file(path)) {
                        Ok(_) => success_count += 1,
                        Err(e2) => {
                            // Clean up partial copy
                            let _ = fs::remove_file(&dest);
                            error_details.push(format!("• {:?}: {}", filename, e2));
                            failed_paths.insert(path.clone());
                        }
                    }
                }
            }
        }

        self.marked_for_deletion.retain(|p| failed_paths.contains(p));

        if success_count > 0 {
            // Remove moved files from groups
            for group in &mut self.groups {
                group.retain(|f| !paths_to_move.contains(&f.path) || failed_paths.contains(&f.path));
            }

            // Clean up empty groups
            let mut i = 0;
            while i < self.groups.len() {
                if self.groups[i].is_empty() {
                    self.groups.remove(i);
                    self.group_infos.remove(i);
                    if self.current_group_idx >= i && self.current_group_idx > 0 {
                        self.current_group_idx -= 1;
                    }
                } else {
                    self.group_infos[i] = analyze_group(&mut self.groups[i], &self.group_by, &self.ext_priorities, self.use_pdqhash);
                    i += 1;
                }
            }

            // Adjust indices
            if self.groups.is_empty() {
                self.current_group_idx = 0;
                self.current_file_idx = 0;
            } else {
                if self.current_group_idx >= self.groups.len() {
                    self.current_group_idx = self.groups.len() - 1;
                }
                if self.current_file_idx >= self.groups[self.current_group_idx].len() {
                    self.current_file_idx = self.groups[self.current_group_idx].len().saturating_sub(1);
                }
            }
            self.selection_changed = true;
        }

        if failed_paths.is_empty() {
            self.set_status(format!("Moved {} files to {:?}", success_count, target_dir), false);
        } else {
            let mut full_msg = format!("Failed to move {} files:\n\n", failed_paths.len());
            full_msg.push_str(&error_details.into_iter().take(5).collect::<Vec<_>>().join("\n"));
            if failed_paths.len() > 5 { full_msg.push_str("\n...and others."); }
            if success_count > 0 {
                full_msg.push_str(&format!("\n\n({} files moved successfully)", success_count));
            }
            full_msg.push_str("\n\n(Press any key to dismiss)");
            self.error_popup = Some(full_msg);
        }
    }
}

/// Returns a map of (dev, ino) -> Vec<&FileMetadata> for files that are hardlinked
pub fn get_hardlink_groups(group: &[FileMetadata]) -> HashMap<(u64, u64), Vec<usize>> {
    let mut groups: HashMap<(u64, u64), Vec<usize>> = HashMap::new();
    for (idx, f) in group.iter().enumerate() {
        if let Some(dev_ino) = f.dev_inode {
            groups.entry(dev_ino).or_default().push(idx);
        }
    }
    // Only keep groups with 2+ files (actual hardlinks)
    groups.retain(|_, v| v.len() > 1);
    groups
}
