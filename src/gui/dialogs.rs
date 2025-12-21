use crate::format_relative_time;
use crate::position;
use crate::scanner;
use crate::state::InputIntent;
use eframe::egui;
use jiff::Timestamp;
use std::cell::RefCell;
use std::fs;
use std::path::Path;

use super::app::GuiApp;
use super::image::{GroupViewState, ViewMode};

/// Handle keyboard input
pub(super) fn handle_input(
    app: &mut GuiApp,
    ctx: &egui::Context,
    intent: &RefCell<Option<InputIntent>>,
    force_panel_resize: &mut bool,
) {
    // Input handling
    if ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
        if app.show_move_input {
            app.show_move_input = false;
            return;
        }
        if app.show_dir_picker {
            app.show_dir_picker = false;
        } else if app.state.show_search {
            *intent.borrow_mut() = Some(InputIntent::CancelSearch);
        } else if app.state.show_confirmation
            || app.state.error_popup.is_some()
            || app.state.renaming.is_some()
            || app.state.show_sort_selection
        {
            *intent.borrow_mut() = Some(InputIntent::Cancel);
        } else {
            *intent.borrow_mut() = Some(InputIntent::Quit);
        }
    }
    if app.show_move_input {
        return;
    }

    // Directory picker navigation
    if app.show_dir_picker {
        if ctx.input(|i| i.key_pressed(egui::Key::ArrowUp)) && app.dir_picker_selection > 0 {
            app.dir_picker_selection -= 1;
            app.dir_picker_scroll_to_selection = true;
        }
        if ctx.input(|i| i.key_pressed(egui::Key::ArrowDown))
            && app.dir_picker_selection + 1 < app.dir_list.len()
        {
            app.dir_picker_selection += 1;
            app.dir_picker_scroll_to_selection = true;
        }
        // PageUp - move up by 10 items
        if ctx.input(|i| i.key_pressed(egui::Key::PageUp)) {
            app.dir_picker_selection = app.dir_picker_selection.saturating_sub(10);
            app.dir_picker_scroll_to_selection = true;
        }
        // PageDown - move down by 10 items
        if ctx.input(|i| i.key_pressed(egui::Key::PageDown)) {
            let max_idx = app.dir_list.len().saturating_sub(1);
            app.dir_picker_selection = (app.dir_picker_selection + 10).min(max_idx);
            app.dir_picker_scroll_to_selection = true;
        }
        // Home - go to first item
        if ctx.input(|i| i.key_pressed(egui::Key::Home)) {
            app.dir_picker_selection = 0;
            app.dir_picker_scroll_to_selection = true;
        }
        // End - go to last item
        if ctx.input(|i| i.key_pressed(egui::Key::End)) {
            app.dir_picker_selection = app.dir_list.len().saturating_sub(1);
            app.dir_picker_scroll_to_selection = true;
        }
        if ctx.input(|i| i.key_pressed(egui::Key::Enter))
            && let Some(selected_dir) = app.dir_list.get(app.dir_picker_selection).cloned()
        {
            app.show_dir_picker = false;
            app.change_directory(selected_dir);
        }
        return;
    } else if !app.state.is_loading
        && app.state.renaming.is_none()
        && !app.state.show_sort_selection
        && !app.state.show_search
    {
        // Calculate total directory count (parent + subdirs) for view mode navigation
        let has_parent =
            app.state.view_mode && app.current_dir.as_ref().and_then(|c| c.parent()).is_some();
        let total_dirs = if app.state.view_mode {
            (if has_parent { 1 } else { 0 }) + app.subdirs.len()
        } else {
            0
        };
        let has_files = !app.state.groups.is_empty() && !app.state.groups[0].is_empty();

        // Handle Up/Left navigation
        if ctx.input(|i| i.key_pressed(egui::Key::ArrowUp) || i.key_pressed(egui::Key::ArrowLeft)) {
            if app.state.view_mode && total_dirs > 0 {
                if let Some(dir_idx) = app.dir_selection_idx {
                    // Already in directory list, move up
                    if dir_idx > 0 {
                        app.dir_selection_idx = Some(dir_idx - 1);
                        app.dir_scroll_to_selection = true;
                    }
                    // At top of directory list, stay there
                } else if app.state.current_file_idx == 0 {
                    // At first file, move to directory list (last directory)
                    app.dir_selection_idx = Some(total_dirs - 1);
                    app.dir_scroll_to_selection = true;
                } else {
                    // Normal file navigation
                    *intent.borrow_mut() = Some(InputIntent::PrevItem);
                }
            } else {
                *intent.borrow_mut() = Some(InputIntent::PrevItem);
            }
        }

        // Handle Down/Right navigation
        if ctx
            .input(|i| i.key_pressed(egui::Key::ArrowDown) || i.key_pressed(egui::Key::ArrowRight))
        {
            if app.state.view_mode && app.dir_selection_idx.is_some() {
                let dir_idx = app.dir_selection_idx.unwrap();
                if dir_idx + 1 < total_dirs {
                    // Move down in directory list
                    app.dir_selection_idx = Some(dir_idx + 1);
                    app.dir_scroll_to_selection = true;
                } else if has_files {
                    // At bottom of directory list, move to first file
                    app.dir_selection_idx = None;
                    app.state.current_file_idx = 0;
                    app.state.selection_changed = true;
                }
                // If no files, stay at last directory
            } else {
                *intent.borrow_mut() = Some(InputIntent::NextItem);
            }
        }

        // Handle Enter to open selected directory
        if ctx.input(|i| i.key_pressed(egui::Key::Enter)) {
            if app.state.view_mode
                && let Some(dir_idx) = app.dir_selection_idx
            {
                // Determine which directory to open
                let dir_to_open = if has_parent {
                    if dir_idx == 0 {
                        // Parent directory
                        app.current_dir.as_ref().and_then(|c| c.parent()).map(|p| p.to_path_buf())
                    } else {
                        // Subdirectory (index adjusted for parent)
                        app.subdirs.get(dir_idx - 1).cloned()
                    }
                } else {
                    // No parent, subdirs start at index 0
                    app.subdirs.get(dir_idx).cloned()
                };
                if let Some(dir) = dir_to_open {
                    app.dir_selection_idx = None;
                    app.change_directory(dir);
                }
            }
        }

        // PageDown - in view mode, handle directories too
        if ctx.input(|i| i.key_pressed(egui::Key::PageDown)) {
            if app.state.view_mode {
                let has_parent = app.current_dir.as_ref().and_then(|c| c.parent()).is_some();
                let total_dirs = (if has_parent { 1 } else { 0 }) + app.subdirs.len();
                let total_files = app.state.groups.first().map(|g| g.len()).unwrap_or(0);
                let page_size = 15;

                if let Some(dir_idx) = app.dir_selection_idx {
                    // Currently in directories
                    let new_idx = dir_idx + page_size;
                    if new_idx < total_dirs {
                        app.dir_selection_idx = Some(new_idx);
                        app.dir_scroll_to_selection = true;
                    } else if total_files > 0 {
                        // Jump to files
                        app.dir_selection_idx = None;
                        let file_offset = new_idx - total_dirs;
                        app.state.current_file_idx = file_offset.min(total_files.saturating_sub(1));
                        app.state.selection_changed = true;
                    } else {
                        // No files, stay at last directory
                        app.dir_selection_idx = Some(total_dirs.saturating_sub(1));
                        app.dir_scroll_to_selection = true;
                    }
                } else {
                    // Currently in files, use normal behavior
                    *intent.borrow_mut() = Some(InputIntent::PageDown);
                }
            } else {
                *intent.borrow_mut() = Some(InputIntent::PageDown);
            }
        }
        if ctx.input(|i| i.modifiers.shift && i.key_pressed(egui::Key::PageDown)) {
            *intent.borrow_mut() = Some(InputIntent::NextGroupByDist);
        }

        // PageUp - in view mode, handle directories too
        if ctx.input(|i| i.key_pressed(egui::Key::PageUp)) {
            if app.state.view_mode {
                let has_parent = app.current_dir.as_ref().and_then(|c| c.parent()).is_some();
                let total_dirs = (if has_parent { 1 } else { 0 }) + app.subdirs.len();
                let page_size = 15;

                if app.dir_selection_idx.is_some() {
                    // Currently in directories
                    let dir_idx = app.dir_selection_idx.unwrap();
                    if dir_idx >= page_size {
                        app.dir_selection_idx = Some(dir_idx - page_size);
                    } else {
                        app.dir_selection_idx = Some(0);
                    }
                    app.dir_scroll_to_selection = true;
                } else if app.state.current_file_idx == 0 && total_dirs > 0 {
                    // At first file, jump to directories
                    app.dir_selection_idx = Some(total_dirs.saturating_sub(1));
                    app.dir_scroll_to_selection = true;
                } else if app.state.current_file_idx < page_size && total_dirs > 0 {
                    // Would go past first file, jump to directories
                    let remaining = page_size - app.state.current_file_idx;
                    app.dir_selection_idx = Some(total_dirs.saturating_sub(remaining).max(0));
                    app.dir_scroll_to_selection = true;
                } else {
                    // Normal file navigation
                    *intent.borrow_mut() = Some(InputIntent::PageUp);
                }
            } else {
                *intent.borrow_mut() = Some(InputIntent::PageUp);
            }
        }
        if ctx.input(|i| i.modifiers.shift && i.key_pressed(egui::Key::PageUp)) {
            *intent.borrow_mut() = Some(InputIntent::PreviousGroupByDist);
        }

        // Home - in view mode, go to first directory (or first file if no dirs)
        if ctx.input(|i| i.key_pressed(egui::Key::Home)) {
            if app.state.view_mode {
                let has_parent = app.current_dir.as_ref().and_then(|c| c.parent()).is_some();
                let total_dirs = (if has_parent { 1 } else { 0 }) + app.subdirs.len();
                if total_dirs > 0 {
                    app.dir_selection_idx = Some(0);
                    app.dir_scroll_to_selection = true;
                } else {
                    *intent.borrow_mut() = Some(InputIntent::Home);
                }
            } else {
                *intent.borrow_mut() = Some(InputIntent::Home);
            }
        }

        // End - in view mode, go to last file (or last directory if no files)
        if ctx.input(|i| i.key_pressed(egui::Key::End)) {
            if app.state.view_mode {
                let total_files = app.state.groups.first().map(|g| g.len()).unwrap_or(0);
                if total_files > 0 {
                    app.dir_selection_idx = None;
                    *intent.borrow_mut() = Some(InputIntent::End);
                } else {
                    let has_parent = app.current_dir.as_ref().and_then(|c| c.parent()).is_some();
                    let total_dirs = (if has_parent { 1 } else { 0 }) + app.subdirs.len();
                    if total_dirs > 0 {
                        app.dir_selection_idx = Some(total_dirs.saturating_sub(1));
                        app.dir_scroll_to_selection = true;
                    }
                }
            } else {
                *intent.borrow_mut() = Some(InputIntent::End);
            }
        }
        if ctx.input(|i| i.modifiers.shift && i.key_pressed(egui::Key::Tab)) {
            *intent.borrow_mut() = Some(InputIntent::PrevGroup);
        } else if ctx.input(|i| i.key_pressed(egui::Key::Tab)) {
            *intent.borrow_mut() = Some(InputIntent::NextGroup);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::Space)) {
            *intent.borrow_mut() = Some(InputIntent::ToggleMark);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::D)) {
            *intent.borrow_mut() = Some(InputIntent::ExecuteDelete);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::H)) {
            *intent.borrow_mut() = Some(InputIntent::ToggleRelativeTime);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::W)) {
            *intent.borrow_mut() = Some(InputIntent::CycleViewMode);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::Z)) {
            *intent.borrow_mut() = Some(InputIntent::CycleZoom);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::R)) {
            *intent.borrow_mut() = Some(InputIntent::StartRename);
        }
        if ctx.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::L)) {
            *intent.borrow_mut() = Some(InputIntent::RefreshDirCache);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::X)) {
            *intent.borrow_mut() = Some(InputIntent::ToggleZoomRelative);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::P)) {
            *intent.borrow_mut() = Some(InputIntent::TogglePathVisibility);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::Delete)) {
            *intent.borrow_mut() = Some(InputIntent::DeleteImmediate);
        }
        // Intercept MoveMarked intent or Key::M
        if ctx.input(|i| i.key_pressed(egui::Key::M)) {
            // Check if there is anything to move at all.
            // We need either Marked Files OR a Current File (fallback).
            let has_marked = !app.state.marked_for_deletion.is_empty();
            let has_current = app.state.get_current_image_path().is_some();

            if !has_marked && !has_current {
                // Nothing to move. Show status immediately.
                app.state.status_message =
                    Some(("No files marked and no file selected.".to_string(), true));
                app.state.status_set_time = Some(std::time::Instant::now());
            } else {
                // Allow Shift+M to force editing the target even if set
                let force_edit = ctx.input(|i| i.modifiers.shift);

                if app.state.move_target.is_some() && !force_edit {
                    *intent.borrow_mut() = Some(InputIntent::MoveMarked);
                } else {
                    app.show_move_input = true;
                    // Pre-fill with existing target if we have one
                    if let Some(ref current) = app.state.move_target {
                        app.move_input = current.to_string_lossy().to_string();
                    } else {
                        app.move_input.clear();
                    }
                }
            }
        }
        if ctx.input(|i| i.key_pressed(egui::Key::S)) {
            *intent.borrow_mut() = Some(InputIntent::ToggleSlideshow);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::F)) {
            *intent.borrow_mut() = Some(InputIntent::ToggleFullscreen);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::O)) {
            *intent.borrow_mut() = Some(InputIntent::RotateCW);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::Y)) {
            *intent.borrow_mut() = Some(InputIntent::FlipHorizontal);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::U)) {
            *intent.borrow_mut() = Some(InputIntent::FlipVertical);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::Backspace)) {
            *intent.borrow_mut() = Some(InputIntent::ResetTransform);
        }
        if ctx.input(|i| i.key_pressed(egui::Key::I)) {
            app.show_histogram = !app.show_histogram;
        }
        if ctx.input(|i| i.key_pressed(egui::Key::E)) {
            app.show_exif = !app.show_exif;
        }

        // N key: Toggle GPS Map panel
        if ctx.input(|i| i.key_pressed(egui::Key::N))
            && !app.state.show_confirmation
            && !app.state.show_move_confirmation
            && !app.state.show_delete_immediate_confirmation
        {
            app.gps_map.toggle();
            if app.gps_map.visible {
                // Auto-select first location from config if none selected
                if app.gps_map.selected_location.is_none() {
                    // AppContext.locations is HashMap<String, Point<f64>>
                    if let Some((name, point)) = app.ctx.locations.iter().next() {
                        app.gps_map.selected_location = Some((name.clone(), *point));
                    }
                }

                // Set initial center on current image if it has GPS
                // Use gps_pos from FileMetadata directly if available (works in view mode)
                let current_file_data = app
                    .state
                    .groups
                    .get(app.state.current_group_idx)
                    .and_then(|g| g.get(app.state.current_file_idx))
                    .map(|f| (f.path.clone(), f.content_hash, f.gps_pos, f.unique_file_id));

                if let Some((path, content_hash, gps_pos, unique_file_id)) = current_file_data {
                    if let Some(pos) = gps_pos {
                        // Fast path: use cached gps_pos
                        app.gps_map.set_initial_center(pos.y(), pos.x());
                    } else if let Some((lat, lon)) =
                        app.get_gps_coords(&path, &content_hash, Some(unique_file_id))
                    {
                        // Slow path: lookup from database or EXIF
                        app.gps_map.set_initial_center(lat, lon);
                    } else if let Some(first_marker) = app.gps_map.markers.first() {
                        // Fallback to first marker if current image has no GPS
                        app.gps_map.set_initial_center(first_marker.lat, first_marker.lon);
                    }
                } else if let Some(first_marker) = app.gps_map.markers.first() {
                    // No current image, center on first marker
                    app.gps_map.set_initial_center(first_marker.lat, first_marker.lon);
                }
                let marker_count = app.gps_map.markers.len();
                app.set_status(format!("GPS Map enabled. {} markers loaded.", marker_count), false);
            } else {
                app.set_status("GPS Map disabled.".to_string(), false);
            }
        }

        if ctx.input(|i| i.key_pressed(egui::Key::G)) {
            // Toggle Time Source
            app.state.use_gps_utc = !app.state.use_gps_utc;
            app.cached_exif = None;
            app.exif_search_cache.clear();
            // Show status
            let mode = if app.state.use_gps_utc { "GPS (UTC)" } else { "EXIF (Local)" };
            app.state.status_message = Some((format!("Sun Position Time: {}", mode), false));
            app.state.status_set_time = Some(std::time::Instant::now());

            // Check fallback immediately for current file
            if app.state.use_gps_utc {
                if let Some(path) = app.state.get_current_image_path() {
                    if !crate::scanner::has_gps_time(path) {
                        app.state.status_message = Some((
                            "Sun Position: GPS Time missing, falling back to Local time."
                                .to_string(),
                            true, // Error color
                        ));
                        app.state.status_set_time = Some(std::time::Instant::now());
                    }
                }
            }
        }

        // View Mode Only
        if app.state.view_mode {
            if ctx.input(|i| i.key_pressed(egui::Key::C)) {
                app.open_dir_picker();
            }
            if ctx.input(|i| i.key_pressed(egui::Key::Period)) {
                app.go_up_directory();
            }
            if ctx.input(|i| i.key_pressed(egui::Key::T)) {
                *intent.borrow_mut() = Some(InputIntent::ShowSortSelection);
            }
        }

        let window_width =
            ctx.input(|i| i.viewport().inner_rect.map(|r| r.width()).unwrap_or(1000.0));
        let delta = window_width * 0.02;

        // V to Shrink panel
        if ctx.input(|i| i.key_pressed(egui::Key::V)) {
            app.panel_width = (app.panel_width - delta).max(96.0);
            *force_panel_resize = true;
        }
        // B to Expand
        if ctx.input(|i| i.key_pressed(egui::Key::B)) {
            app.panel_width = (app.panel_width + delta).min(window_width * 0.8);
            *force_panel_resize = true;
        }
        // Search
        if ctx.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::F)) {
            *intent.borrow_mut() = Some(InputIntent::StartSearch);
            app.search_input.clear();
            app.search_focus_requested = false;
        }
        if ctx.input(|i| i.key_pressed(egui::Key::F3)) {
            if ctx.input(|i| i.modifiers.shift) {
                *intent.borrow_mut() = Some(InputIntent::PrevSearchResult);
            } else {
                *intent.borrow_mut() = Some(InputIntent::NextSearchResult);
            }
        }
    }
}

/// Handle dialogs and apply intents
pub(super) fn handle_dialogs(
    app: &mut GuiApp,
    ctx: &egui::Context,
    force_panel_resize: &mut bool,
    intent: &RefCell<Option<InputIntent>>,
) {
    let pending = intent.borrow().clone();
    if let Some(i) = pending {
        let requires_cache_rebuild = matches!(
            i,
            InputIntent::DeleteImmediate
                | InputIntent::ConfirmDelete
                | InputIntent::ConfirmDeleteImmediate
                | InputIntent::ConfirmMoveMarked
                | InputIntent::ChangeSortOrder(_)
                | InputIntent::SubmitRename(_)
                | InputIntent::RefreshDirCache
        );

        if requires_cache_rebuild {
            app.cache_dirty = true;
        }

        match i {
            InputIntent::CycleViewMode => {
                app.update_view_state(|v| {
                    v.mode = match v.mode {
                        ViewMode::FitWindow => ViewMode::FitWidth,
                        ViewMode::FitWidth => ViewMode::FitHeight,
                        _ => ViewMode::FitWindow,
                    };
                });
            }
            InputIntent::CycleZoom => {
                app.update_view_state(|v| {
                    v.mode = match v.mode {
                        ViewMode::FitWindow => ViewMode::ManualZoom(1.0), // 1:1 native pixels
                        ViewMode::ManualZoom(z) if (z - 1.0).abs() < 0.1 => {
                            ViewMode::ManualZoom(2.0)
                        }
                        ViewMode::ManualZoom(z) if (z - 2.0).abs() < 0.1 => {
                            ViewMode::ManualZoom(4.0)
                        }
                        ViewMode::ManualZoom(z) if (z - 4.0).abs() < 0.1 => {
                            ViewMode::ManualZoom(8.0)
                        }
                        ViewMode::ManualZoom(_) => ViewMode::FitWindow,
                        _ => ViewMode::ManualZoom(1.0),
                    };
                });
            }
            InputIntent::StartRename => {
                if let Some(path) = app.state.get_current_image_path() {
                    app.rename_input =
                        path.file_name().unwrap_or_default().to_string_lossy().to_string();
                    app.completion_candidates.clear();
                    app.completion_index = 0;
                    app.state.handle_input(i);
                }
            }
            _ => app.state.handle_input(i),
        }
    }

    // Dialogs (Confirmation, Rename, etc.)
    // Handle Y/N keys for confirmation dialogs
    if app.state.show_confirmation {
        if ctx.input(|i| i.key_pressed(egui::Key::Y)) {
            app.state.handle_input(InputIntent::ConfirmDelete);
        } else if ctx.input(|i| i.key_pressed(egui::Key::N)) {
            app.state.handle_input(InputIntent::Cancel);
        }
        egui::Window::new("Confirm Deletion").collapsible(false).show(ctx, |ui| {
            let marked_count = app.state.marked_for_deletion.len();
            let use_trash = app.state.use_trash;
            ui.label(format!(
                "Are you sure you want to {} {} files?",
                if use_trash { "trash" } else { "permanently delete" },
                marked_count
            ));
            if ui.button("Yes (y)").clicked() {
                app.state.handle_input(InputIntent::ConfirmDelete);
                app.cache_dirty = true;
            }
            if ui.button("No (n)").clicked() {
                app.state.handle_input(InputIntent::Cancel);
            }
        });
    }

    if app.state.show_delete_immediate_confirmation {
        if ctx.input(|i| i.key_pressed(egui::Key::Y)) {
            app.state.handle_input(InputIntent::ConfirmDeleteImmediate);
            app.cache_dirty = true;
        } else if ctx.input(|i| i.key_pressed(egui::Key::N)) {
            app.state.handle_input(InputIntent::Cancel);
        }
        egui::Window::new("Confirm Delete").collapsible(false).show(ctx, |ui| {
            let filename = app
                .state
                .get_current_image_path()
                .map(|p| p.file_name().unwrap_or_default().to_string_lossy().to_string())
                .unwrap_or_default();
            ui.label(format!("Delete current file?\n{}", filename));
            if ui.button("Yes (y)").clicked() {
                app.state.handle_input(InputIntent::ConfirmDeleteImmediate);
                app.cache_dirty = true;
            }
            if ui.button("No (n)").clicked() {
                app.state.handle_input(InputIntent::Cancel);
            }
        });
    }

    if app.state.show_move_confirmation {
        if ctx.input(|i| i.key_pressed(egui::Key::Y)) {
            app.state.handle_input(InputIntent::ConfirmMoveMarked);
            app.cache_dirty = true;
        } else if ctx.input(|i| i.key_pressed(egui::Key::N)) {
            app.state.handle_input(InputIntent::Cancel);
        }
        egui::Window::new("Confirm Move").collapsible(false).show(ctx, |ui| {
            let target =
                app.state.move_target.as_ref().map(|p| p.display().to_string()).unwrap_or_default();

            let msg = if app.state.marked_for_deletion.is_empty() {
                if let Some(p) = app.state.get_current_image_path() {
                    let name = p.file_name().unwrap_or_default().to_string_lossy();
                    format!("Move current file '{}' to:\n{}", name, target)
                } else {
                    format!("Move 0 files to:\n{}", target)
                }
            } else {
                format!("Move {} marked files to:\n{}", app.state.marked_for_deletion.len(), target)
            };

            ui.label(msg);
            ui.horizontal(|ui| {
                if ui.button("Yes (y)").clicked() {
                    app.state.handle_input(InputIntent::ConfirmMoveMarked);
                    app.cache_dirty = true;
                }
                if ui.button("No (n)").clicked() {
                    app.state.handle_input(InputIntent::Cancel);
                }

                // Add "Change Target" button
                if ui.button("Change Target...").clicked() {
                    // Close this confirmation
                    app.state.show_move_confirmation = false;
                    // Open input dialog
                    app.show_move_input = true;
                    // Pre-fill input with the bad target so it can be edited
                    app.move_input = target;
                }
            });
        });
    }

    // Move Input Dialog
    if app.show_move_input {
        let mut submit = false;
        let mut cancel = false;
        let mut request_focus_back = false;

        egui::Window::new("Move to Directory").collapsible(false).show(ctx, |ui| {
            ui.label("Enter destination directory:");

            // Color logic: RED if directory doesn't exist, Default (text color) otherwise
            let path_exists = Path::new(&app.move_input).is_dir();
            let text_color = if !path_exists && !app.move_input.is_empty() {
                egui::Color32::RED
            } else {
                ui.visuals().text_color()
            };

            let res = ui.add(
                egui::TextEdit::singleline(&mut app.move_input)
                    .text_color(text_color)
                    .desired_width(300.0),
            );

            if !app.state.show_sort_selection && !app.state.show_confirmation {
                res.request_focus();
            }

            // Tab Completion (DIRECTORIES ONLY)
            if ui.input(|i| i.key_pressed(egui::Key::Tab)) {
                request_focus_back = true;
                let path_buf = std::path::PathBuf::from(&app.move_input);
                let (parent, prefix) = if app.move_input.ends_with(std::path::MAIN_SEPARATOR) {
                    (Some(path_buf.as_path()), "".to_string())
                } else {
                    (
                        path_buf.parent(),
                        path_buf.file_name().unwrap_or_default().to_string_lossy().to_string(),
                    )
                };

                if let Some(parent_dir) = parent {
                    // Check if we need to refresh candidates
                    let prev_idx = if !app.move_completion_candidates.is_empty() {
                        (app.move_completion_index + app.move_completion_candidates.len() - 1)
                            % app.move_completion_candidates.len()
                    } else {
                        0
                    };

                    let input_matches_candidate = !app.move_completion_candidates.is_empty()
                        && app.move_completion_candidates[prev_idx] == app.move_input;

                    if app.move_completion_candidates.is_empty() || !input_matches_candidate {
                        app.move_completion_candidates.clear();
                        app.move_completion_index = 0;
                        if let Ok(entries) = fs::read_dir(parent_dir) {
                            for entry in entries.flatten() {
                                // Filter: ONLY DIRECTORIES
                                if let Ok(ft) = entry.file_type() {
                                    if ft.is_dir() {
                                        let name = entry.path().to_string_lossy().to_string();
                                        if name.starts_with(&app.move_input)
                                            || entry
                                                .file_name()
                                                .to_string_lossy()
                                                .starts_with(&prefix)
                                        {
                                            // Store full path for convenience
                                            app.move_completion_candidates.push(name);
                                        }
                                    }
                                }
                            }
                            app.move_completion_candidates.sort();
                        }
                    }

                    if !app.move_completion_candidates.is_empty() {
                        app.move_input =
                            app.move_completion_candidates[app.move_completion_index].clone();
                        app.move_completion_index =
                            (app.move_completion_index + 1) % app.move_completion_candidates.len();
                    }
                }
            }

            if ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                submit = true;
            }
            if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                cancel = true;
            }

            if request_focus_back {
                res.request_focus();
            }

            ui.horizontal(|ui| {
                if ui.button("Move Here").clicked() {
                    submit = true;
                }
                if ui.button("Cancel").clicked() {
                    cancel = true;
                }
            });
        });

        if submit {
            let input_path = std::path::PathBuf::from(&app.move_input);
            // Resolve relative paths against current_dir (currently displayed directory)
            let target_path = if input_path.is_absolute() {
                input_path
            } else if let Some(ref current) = app.current_dir {
                current.join(&input_path)
            } else {
                input_path
            };
            if target_path.is_dir() {
                // Set the target and trigger the standard confirmation flow
                app.state.move_target = Some(target_path);
                app.show_move_input = false;
                app.state.handle_input(InputIntent::MoveMarked);
            } else {
                app.state.error_popup = Some("Target is not a valid directory.".to_string());
            }
        }
        if cancel {
            app.show_move_input = false;
        }
    }

    // Search Dialog with Fixes
    if app.state.show_search {
        let mut submit = false;
        let mut cancel = false;

        egui::Window::new("Find String (Regex)").collapsible(false).show(ctx, |ui| {
            ui.label("Search options:");
            ui.label("• Filename regex (default)");
            ui.label("• 'sun_az=170-190' (numeric range)");
            ui.label("• 'sun_alt=-3-3' (numeric range)");

            let res = ui.text_edit_singleline(&mut app.search_input);

            if !app.search_focus_requested {
                res.request_focus();
                app.search_focus_requested = true;
            }

            ui.horizontal(|ui| {
                ui.checkbox(&mut app.state.search_include_exif, "Include EXIF");
                ui.separator();
                ui.checkbox(&mut app.search_sun_azimuth_enabled, "Calc Azimuth");
                ui.checkbox(&mut app.search_sun_altitude_enabled, "Calc Altitude");
            });

            ui.small("Checking Azimuth/Altitude enables calculation for all files (slower).");

            // Check both has_focus (typing) and lost_focus (committed via Enter)
            let enter_pressed = ui.input(|i| i.key_pressed(egui::Key::Enter));
            if enter_pressed && (res.has_focus() || res.lost_focus()) {
                submit = true;
            }

            ui.horizontal(|ui| {
                if ui.button("Find").clicked() {
                    submit = true;
                }
                if ui.button("Cancel").clicked() {
                    cancel = true;
                }
            });
        });

        if submit {
            let query = app.search_input.clone();
            perform_search_with_cache(app, query);
        }
        if cancel {
            app.state.handle_input(InputIntent::CancelSearch);
        }
    }

    if app.state.renaming.is_some() {
        let mut submit = false;
        let mut cancel = false;
        let mut request_focus_back = false;

        egui::Window::new("Rename").collapsible(false).show(ctx, |ui| {
            let res = ui.text_edit_singleline(&mut app.rename_input);
            if !app.state.show_sort_selection && !app.state.show_confirmation {
                res.request_focus();
            }

            if ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                submit = true;
            }

            if ui.input(|i| i.key_pressed(egui::Key::Tab)) {
                request_focus_back = true;
                let parent = if let Some(state) = &app.state.renaming {
                    state.original_path.parent().map(|p| p.to_path_buf())
                } else {
                    None
                };

                if let Some(parent_dir) = parent {
                    // Calculate previous index to check if input matches what we last auto-completed
                    let prev_idx = if !app.completion_candidates.is_empty() {
                        (app.completion_index + app.completion_candidates.len() - 1)
                            % app.completion_candidates.len()
                    } else {
                        0
                    };

                    // Check if the current input matches the candidate we just showed.
                    // This confirms the user hasn't typed something new manually.
                    let input_matches_candidate = !app.completion_candidates.is_empty()
                        && app.completion_candidates[prev_idx] == app.rename_input;

                    // If empty or user typed something new, scan for new candidates
                    if app.completion_candidates.is_empty() || !input_matches_candidate {
                        app.completion_candidates.clear();
                        app.completion_index = 0;
                        if let Ok(entries) = fs::read_dir(&parent_dir) {
                            let prefix = app.rename_input.clone();
                            for entry in entries.flatten() {
                                let name = entry.file_name().to_string_lossy().to_string();
                                if name.starts_with(&prefix) {
                                    app.completion_candidates.push(name);
                                }
                            }
                            app.completion_candidates.sort();
                        }
                    }

                    // Apply the next completion
                    if !app.completion_candidates.is_empty() {
                        app.rename_input = app.completion_candidates[app.completion_index].clone();
                        app.completion_index =
                            (app.completion_index + 1) % app.completion_candidates.len();
                    }
                }
            }

            if request_focus_back {
                res.request_focus();
            }

            if ui.button("Rename").clicked() {
                submit = true;
            }
            if ui.button("Cancel").clicked() {
                cancel = true;
            }
        });

        if submit {
            // Clean up the old texture from cache immediately
            if let Some(state) = &app.state.renaming {
                app.raw_cache.remove(&state.original_path);
            }

            app.state.handle_input(InputIntent::SubmitRename(app.rename_input.clone()));
            app.completion_candidates.clear();

            app.last_preload_pos = None;
            app.cache_dirty = true; // Rebuild cache after rename
        }
        if cancel {
            app.state.handle_input(InputIntent::Cancel);
            app.completion_candidates.clear();
        }
    }

    // Sort Selection Dialog
    if app.state.show_sort_selection {
        let mut selected_sort = None;

        egui::Window::new("Sort Order").collapsible(false).show(ctx, |ui| {
            ui.label("Select sort order (or press 1-9):");
            ui.separator();

            let options = [
                ("1. Name (A-Z)", "name", egui::Key::Num1),
                ("2. Name (Z-A)", "name-desc", egui::Key::Num2),
                ("3. Name Natural (A-Z)", "name-natural", egui::Key::Num3),
                ("4. Name Natural (Z-A)", "name-natural-desc", egui::Key::Num4),
                ("5. Date (Oldest First)", "date", egui::Key::Num5),
                ("6. Date (Newest First)", "date-desc", egui::Key::Num6),
                ("7. Size (Smallest First)", "size", egui::Key::Num7),
                ("8. Size (Largest First)", "size-desc", egui::Key::Num8),
                ("9. Random", "random", egui::Key::Num9),
            ];

            for (label, value, key) in options {
                // Check if button clicked OR corresponding number key pressed
                if ui.button(label).clicked() || ctx.input(|i| i.key_pressed(key)) {
                    selected_sort = Some(value.to_string());
                }
            }

            ui.separator();
            if ui.button("Cancel (Esc)").clicked() {
                selected_sort = Some("CANCEL".to_string());
            }
        });

        // Handle the selection after the UI closure to avoid borrow conflicts
        if let Some(sort) = selected_sort {
            if sort == "CANCEL" {
                app.state.handle_input(InputIntent::Cancel);
            } else {
                // Update stored preference for future scans
                app.view_mode_sort = Some(sort.clone());
                // Explicitly sort subdirectories (AppState only handles files)
                scanner::sort_directories(&mut app.subdirs, &sort);
                app.state.handle_input(InputIntent::ChangeSortOrder(sort));
                app.cache_dirty = true;
            }
        }
    }

    // Directory picker dialog (view mode)
    if app.show_dir_picker {
        let mut selected_dir: Option<std::path::PathBuf> = None;
        let mut clicked_idx: Option<usize> = None;
        let show_relative = app.state.show_relative_times;
        let scroll_to_sel = app.dir_picker_scroll_to_selection;

        egui::Window::new("Select Directory")
            .collapsible(false)
            .resizable(true)
            .default_width(650.0)
            .min_width(450.0)
            .show(ctx, |ui| {
                ui.label("Use ↑/↓/PgUp/PgDn/Home/End to navigate, Enter to select, Esc to cancel");
                ui.separator();

                egui::ScrollArea::vertical().max_height(400.0).auto_shrink([false, false]).show(
                    ui,
                    |ui| {
                        let available_w = ui.available_width();
                        ui.set_min_width(available_w);

                        for (idx, dir_path) in app.dir_list.iter().enumerate() {
                            let is_selected = idx == app.dir_picker_selection;
                            let is_parent = idx == 0
                                && app.current_dir.as_ref().and_then(|c| c.parent()).is_some();

                            // Get modification time
                            let mod_time_str = if let Ok(meta) = fs::metadata(dir_path) {
                                if let Ok(modified) = meta.modified() {
                                    let dt: chrono::DateTime<chrono::Utc> = modified.into();
                                    if show_relative {
                                        let ts = Timestamp::from_second(dt.timestamp()).unwrap();
                                        format_relative_time(ts)
                                    } else {
                                        dt.format("%Y-%m-%d %H:%M").to_string()
                                    }
                                } else {
                                    String::new()
                                }
                            } else {
                                String::new()
                            };

                            let dir_name = if is_parent {
                                "📁 .. ".to_string()
                            } else {
                                format!(
                                    "📁 {}",
                                    dir_path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| dir_path.to_string_lossy().to_string())
                                )
                            };

                            // Layout: directory name (2/3) + modification time (1/3)
                            let _row_rect = ui.available_rect_before_wrap();
                            let row_height = ui.text_style_height(&egui::TextStyle::Body) + 4.0;
                            let (rect, resp) = ui.allocate_exact_size(
                                egui::vec2(available_w, row_height),
                                egui::Sense::click(),
                            );

                            // Draw selection background
                            if is_selected {
                                ui.painter().rect_filled(rect, 2.0, ui.visuals().selection.bg_fill);
                            } else if resp.hovered() {
                                ui.painter().rect_filled(
                                    rect,
                                    2.0,
                                    ui.visuals().widgets.hovered.bg_fill,
                                );
                            }

                            // Draw directory name (left 2/3)
                            let name_rect = egui::Rect::from_min_size(
                                rect.min,
                                egui::vec2(available_w * 0.67, row_height),
                            );
                            let text_color = if is_parent {
                                egui::Color32::YELLOW
                            } else {
                                egui::Color32::LIGHT_BLUE
                            };
                            ui.painter().text(
                                name_rect.left_center() + egui::vec2(4.0, 0.0),
                                egui::Align2::LEFT_CENTER,
                                &dir_name,
                                egui::FontId::default(),
                                text_color,
                            );

                            // Draw modification time (right 1/3)
                            let time_rect = egui::Rect::from_min_size(
                                rect.min + egui::vec2(available_w * 0.67, 0.0),
                                egui::vec2(available_w * 0.33, row_height),
                            );
                            ui.painter().text(
                                time_rect.right_center() - egui::vec2(4.0, 0.0),
                                egui::Align2::RIGHT_CENTER,
                                &mod_time_str,
                                egui::FontId::new(11.0, egui::FontFamily::Monospace),
                                egui::Color32::GRAY,
                            );

                            // Single click selects, double click opens
                            if resp.clicked() {
                                clicked_idx = Some(idx);
                            }
                            if resp.double_clicked() {
                                selected_dir = Some(dir_path.clone());
                            }

                            // Only scroll to selected item when keyboard navigation triggered it
                            if is_selected && scroll_to_sel {
                                resp.scroll_to_me(Some(egui::Align::Center));
                            }
                        }

                        if app.dir_list.is_empty() {
                            ui.label("No subdirectories found");
                        }
                    },
                );

                ui.separator();
                ui.horizontal(|ui| {
                    if ui.button("Open (Enter)").clicked() {
                        if let Some(dir) = app.dir_list.get(app.dir_picker_selection).cloned() {
                            selected_dir = Some(dir);
                        }
                    }
                    if ui.button("Cancel (Esc)").clicked() {
                        app.show_dir_picker = false;
                    }
                });
            });

        // Clear scroll flag after rendering
        app.dir_picker_scroll_to_selection = false;

        // Handle single click - update selection index
        if let Some(idx) = clicked_idx {
            app.dir_picker_selection = idx;
        }

        // Apply deferred directory change (from double-click or Enter)
        if let Some(dir) = selected_dir {
            app.show_dir_picker = false;
            app.change_directory(dir);
        }
    }

    // Slideshow
    if let Some(interval) = app.state.slideshow_interval
        && !app.state.slideshow_paused
        && !app.state.is_loading
        && !app.state.groups.is_empty()
    {
        let should_advance = match app.slideshow_last_advance {
            Some(last) => last.elapsed().as_secs_f32() >= interval,
            None => true,
        };
        if should_advance {
            app.slideshow_last_advance = Some(std::time::Instant::now());
            app.state.next_item();
            app.state.selection_changed = true;
        }
        ctx.request_repaint_after(std::time::Duration::from_secs_f32(0.1));
    }

    if let Some(err_text) = app.state.error_popup.clone() {
        egui::Window::new("Error").show(ctx, |ui| {
            ui.label(err_text);
            if ui.button("OK").clicked() {
                app.state.handle_input(InputIntent::Cancel);
            }
        });
    }
}

/// Perform search with EXIF caching and special range queries
fn perform_search_with_cache(app: &mut GuiApp, query: String) {
    use regex::RegexBuilder;

    app.state.search_results.clear();
    if query.is_empty() {
        app.state.show_search = false;
        return;
    }

    // 1. Check for Special Range Search Syntax (sun_az=X-Y or sun_alt=X-Y)
    let mut sun_range_search: Option<(bool, f64, f64)> = None; // (is_azimuth, min, max)

    let lower_query = query.to_lowercase();
    let (is_az, rest) = if let Some(stripped) = lower_query.strip_prefix("sun_az=") {
        (true, stripped)
    } else if let Some(stripped) = lower_query.strip_prefix("sun_alt=") {
        (false, stripped)
    } else {
        (false, "")
    };

    if !rest.is_empty() {
        // Regex to strictly parse two numbers separated by a hyphen
        // Matches: start, optional minus, digits/dots, hyphen, optional minus, digits/dots, end
        let range_re = regex::Regex::new(r"^(-?[\d\.]+)-(-?[\d\.]+)$").unwrap();

        if let Some(caps) = range_re.captures(rest) {
            if let (Ok(min), Ok(max)) = (caps[1].parse::<f64>(), caps[2].parse::<f64>()) {
                sun_range_search = Some((is_az, min, max));
            }
        }
    }

    // 2. Compile Regex (Conditionally)
    let re = if sun_range_search.is_none() {
        match RegexBuilder::new(&query).case_insensitive(true).build() {
            Ok(r) => Some(r),
            Err(e) => {
                app.state.error_popup = Some(format!("Invalid Regex:\n{}", e));
                return;
            }
        }
    } else {
        None
    };

    let include_exif = app.state.search_include_exif;
    // If range search is active or specific checkboxes enabled, we MUST fetch sun position
    let include_sun = sun_range_search.is_some()
        || app.search_sun_azimuth_enabled
        || app.search_sun_altitude_enabled;
    let use_gps = app.state.use_gps_utc;

    // Base tags
    let mut search_tag_names: Vec<String> = vec![
        "Make",
        "Model",
        "LensModel",
        "LensMake",
        "Software",
        "Artist",
        "Copyright",
        "DateTimeOriginal",
        "DateTimeDigitized",
        "ExposureTime",
        "FNumber",
        "ISO",
        "FocalLength",
        "FocalLength35mm",
        "ExposureProgram",
        "MeteringMode",
        "Flash",
        "WhiteBalance",
        "ExposureBias",
        "ColorSpace",
        "Contrast",
        "Saturation",
        "Sharpness",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    // Add DerivedSunPosition if needed
    if include_sun {
        search_tag_names.push("DerivedSunPosition".to_string());
    }

    for (g_idx, group) in app.state.groups.iter().enumerate() {
        for (f_idx, file) in group.iter().enumerate() {
            // Skip deleted files
            if !file.path.exists() {
                app.exif_search_cache.remove(&file.path);
                continue;
            }

            // Handle Range Search (Sun Position)
            if let Some((is_azimuth_search, min_val, max_val)) = sun_range_search {
                // We need the EXIF data including derived sun position
                let exif_tags = if let Some(cached) = app.exif_search_cache.get(&file.path)
                    && cached.iter().any(|(k, _)| k == "Sun Position")
                {
                    cached.clone()
                } else {
                    let mut tags =
                        scanner::get_exif_tags(&file.path, &search_tag_names, false, use_gps);
                    // Ensure country is also there if we are rebuilding cache
                    let country_tags = scanner::get_exif_tags(
                        &file.path,
                        &["DerivedCountry".to_string()],
                        false,
                        use_gps,
                    );
                    tags.extend(country_tags);
                    app.exif_search_cache.insert(file.path.clone(), tags.clone());
                    tags
                };

                // Find Sun Position tag
                if let Some((_, val_str)) = exif_tags.iter().find(|(k, _)| k == "Sun Position") {
                    if let Some((alt, az)) = position::parse_sun_pos_string(val_str) {
                        let val_to_check = if is_azimuth_search { az } else { alt };
                        if val_to_check >= min_val && val_to_check <= max_val {
                            let type_str = if is_azimuth_search { "Azimuth" } else { "Altitude" };
                            app.state.search_results.push((
                                g_idx,
                                f_idx,
                                format!("Sun {}", type_str),
                            ));
                        }
                    }
                }
                continue; // Skip standard regex check for this file if doing range search
            }

            // Standard Regex Search
            let name = file.path.file_name().unwrap_or_default().to_string_lossy();

            // Only use regex if we have one (i.e., not a range search)
            if let Some(re_ref) = &re {
                if re_ref.is_match(&name) {
                    app.state.search_results.push((g_idx, f_idx, "Filename".to_string()));
                    continue;
                }

                if include_exif {
                    // Get EXIF data
                    let exif_tags = if let Some(cached) = app.exif_search_cache.get(&file.path) {
                        cached.clone()
                    } else {
                        let mut tags =
                            scanner::get_exif_tags(&file.path, &search_tag_names, false, use_gps);
                        let country_tags = scanner::get_exif_tags(
                            &file.path,
                            &["DerivedCountry".to_string()],
                            false,
                            false,
                        );
                        tags.extend(country_tags);
                        app.exif_search_cache.insert(file.path.clone(), tags.clone());
                        tags
                    };

                    for (tag_name, tag_value) in &exif_tags {
                        if re_ref.is_match(tag_value) {
                            // Use display name
                            let display_name = if tag_name == "DerivedCountry" {
                                "Country"
                            } else if tag_name == "DerivedSunPosition" {
                                "Sun Position"
                            } else {
                                tag_name
                            };
                            app.state.search_results.push((g_idx, f_idx, display_name.to_string()));
                            break;
                        }
                    }
                }
            }
        }
    }

    if !app.state.search_results.is_empty() {
        app.state.show_search = false;
        app.state.current_search_match = 0;
        let (g, f, ref match_source) = app.state.search_results[0];
        app.state.current_group_idx = g;
        app.state.current_file_idx = f;
        app.state.selection_changed = true;
        app.state.status_message = Some((
            format!(
                "Found {} matches. Match 1/{} in [{}]. (F3/Shift+F3 to nav)",
                app.state.search_results.len(),
                app.state.search_results.len(),
                match_source
            ),
            false,
        ));
        app.state.status_set_time = Some(std::time::Instant::now());
    } else {
        let source = if include_exif { "filenames or EXIF data" } else { "filenames" };
        app.state.error_popup = Some(format!("No matches found in {} for:\n'{}'", source, query));
    }
}
