use std::fs;
use std::io::{self, Stdout};
use std::time::Duration; // Added for directory scanning

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use jiff::Timestamp;
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
};

use crate::GroupStatus;
use crate::format_relative_time;
use crate::state::{
    AppState, InputIntent, format_path_depth, get_bit_identical_counts, get_hardlink_groups,
};

pub struct TuiApp {
    state: AppState,
    list_state: ListState,
    view_height: usize,
    rename_buffer: String,
    show_move_input: bool,
    move_buffer: String,
    move_completion_candidates: Vec<String>,
    move_completion_index: usize,
    search_buffer: String,
    // Completion state
    completion_candidates: Vec<String>,
    completion_index: usize,
}

impl TuiApp {
    pub fn new(state: AppState) -> Self {
        let mut list_state = ListState::default();
        if !state.groups.is_empty() {
            list_state.select(Some(0));
        }
        Self {
            state,
            list_state,
            view_height: 0,
            rename_buffer: String::new(),
            show_move_input: false,
            move_buffer: String::new(),
            move_completion_candidates: Vec::new(),
            move_completion_index: 0,
            search_buffer: String::new(),
            completion_candidates: Vec::new(),
            completion_index: 0,
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;

        let res = self.run_loop(&mut stdout);

        disable_raw_mode()?;
        execute!(stdout, LeaveAlternateScreen)?;
        res
    }

    fn run_loop(&mut self, terminal: &mut Stdout) -> io::Result<()> {
        let mut tui = Terminal::new(CrosstermBackend::new(terminal))?;

        while !self.state.exit_requested {
            tui.draw(|frame| self.render(frame))?;

            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                self.handle_key(key.code, key.modifiers);
            }

            // Sync list state with app state
            self.sync_list_state();
        }
        Ok(())
    }

    fn sync_list_state(&mut self) {
        if self.state.groups.is_empty() {
            self.list_state.select(None);
            return;
        }
        let mut abs_idx = 0;
        for i in 0..self.state.current_group_idx {
            abs_idx += 1 + self.state.groups[i].len();
        }
        abs_idx += 1 + self.state.current_file_idx;
        self.list_state.select(Some(abs_idx));
    }

    fn handle_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        // Error Popup
        if self.state.error_popup.is_some() {
            self.state.handle_input(InputIntent::Cancel);
            return;
        }

        // Move
        if self.show_move_input {
            match code {
                KeyCode::Esc => {
                    self.show_move_input = false;
                    self.move_buffer.clear();
                }
                KeyCode::Enter => {
                    let path = std::path::PathBuf::from(&self.move_buffer);
                    if path.is_dir() {
                        self.state.move_target = Some(path);
                        self.show_move_input = false;
                        self.state.handle_input(InputIntent::MoveMarked);
                    } else {
                        // Flash error or set error state
                    }
                }
                KeyCode::Backspace => {
                    self.move_buffer.pop();
                }
                KeyCode::Char(c) => {
                    self.move_buffer.push(c);
                }
                KeyCode::Tab => {
                    // Similar Directory-Only Completion Logic as GUI
                    let path_buf = std::path::PathBuf::from(&self.move_buffer);
                    let (parent, prefix) = if self.move_buffer.ends_with(std::path::MAIN_SEPARATOR)
                    {
                        (Some(path_buf.as_path()), "".to_string())
                    } else {
                        (
                            path_buf.parent(),
                            path_buf.file_name().unwrap_or_default().to_string_lossy().to_string(),
                        )
                    };

                    if let Some(parent_dir) = parent {
                        let prev_idx = if !self.move_completion_candidates.is_empty() {
                            (self.move_completion_index + self.move_completion_candidates.len() - 1)
                                % self.move_completion_candidates.len()
                        } else {
                            0
                        };
                        let match_candidate = !self.move_completion_candidates.is_empty()
                            && self.move_completion_candidates[prev_idx] == self.move_buffer;

                        if self.move_completion_candidates.is_empty() || !match_candidate {
                            self.move_completion_candidates.clear();
                            self.move_completion_index = 0;
                            if let Ok(entries) = fs::read_dir(parent_dir) {
                                for entry in entries.flatten() {
                                    if let Ok(ft) = entry.file_type()
                                        && ft.is_dir()
                                    {
                                        let name = entry.path().to_string_lossy().to_string();
                                        // Basic prefix check
                                        if name.starts_with(&self.move_buffer)
                                            || entry
                                                .file_name()
                                                .to_string_lossy()
                                                .starts_with(&prefix)
                                        {
                                            self.move_completion_candidates.push(name);
                                        }
                                    }
                                }
                                self.move_completion_candidates.sort();
                            }
                        }
                        if !self.move_completion_candidates.is_empty() {
                            self.move_buffer =
                                self.move_completion_candidates[self.move_completion_index].clone();
                            self.move_completion_index = (self.move_completion_index + 1)
                                % self.move_completion_candidates.len();
                        }
                    }
                }
                _ => {}
            }
            return;
        }

        // Renaming Input
        if self.state.renaming.is_some() {
            match code {
                KeyCode::Esc => {
                    self.state.handle_input(InputIntent::Cancel);
                    self.rename_buffer.clear();
                    self.completion_candidates.clear();
                }
                KeyCode::Enter => {
                    self.state.handle_input(InputIntent::SubmitRename(self.rename_buffer.clone()));
                    self.rename_buffer.clear();
                    self.completion_candidates.clear();
                }
                KeyCode::Backspace => {
                    self.rename_buffer.pop();
                }
                KeyCode::Char(c) => {
                    self.rename_buffer.push(c);
                }
                KeyCode::Tab => {
                    let parent = if let Some(state) = &self.state.renaming {
                        state.original_path.parent().map(|p| p.to_path_buf())
                    } else {
                        None
                    };

                    if let Some(parent_dir) = parent {
                        // Calculate previous index to check if input matches what we last auto-completed
                        let prev_idx = if !self.completion_candidates.is_empty() {
                            (self.completion_index + self.completion_candidates.len() - 1)
                                % self.completion_candidates.len()
                        } else {
                            0
                        };

                        // Check if the current input matches the candidate we just showed.
                        let input_matches_candidate = !self.completion_candidates.is_empty()
                            && self.completion_candidates[prev_idx] == self.rename_buffer;

                        // If empty or user typed something new, scan for new candidates
                        if self.completion_candidates.is_empty() || !input_matches_candidate {
                            self.completion_candidates.clear();
                            self.completion_index = 0;
                            if let Ok(entries) = fs::read_dir(&parent_dir) {
                                let prefix = self.rename_buffer.clone();
                                for entry in entries.flatten() {
                                    let name = entry.file_name().to_string_lossy().to_string();
                                    if name.starts_with(&prefix) {
                                        self.completion_candidates.push(name);
                                    }
                                }
                                self.completion_candidates.sort();
                            }
                        }

                        // Apply the next completion
                        if !self.completion_candidates.is_empty() {
                            self.rename_buffer =
                                self.completion_candidates[self.completion_index].clone();
                            self.completion_index =
                                (self.completion_index + 1) % self.completion_candidates.len();
                        }
                    }
                }
                _ => {}
            }
            return;
        }

        // 3. Delete Confirmation
        if self.state.show_confirmation {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.state.handle_input(InputIntent::ConfirmDelete)
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.state.handle_input(InputIntent::Cancel)
                }
                _ => {}
            }
            return;
        }

        // 4. Move Confirmation
        if self.state.show_move_confirmation {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.state.handle_input(InputIntent::ConfirmMoveMarked)
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.state.handle_input(InputIntent::Cancel)
                }
                _ => {}
            }
            return;
        }

        // 5. Delete Immediate Confirmation
        if self.state.show_delete_immediate_confirmation {
            match code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.state.handle_input(InputIntent::ConfirmDeleteImmediate)
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.state.handle_input(InputIntent::Cancel)
                }
                _ => {}
            }
            return;
        }

        // 6. Sort Selection Menu
        if self.state.show_sort_selection {
            match code {
                KeyCode::Char('1') => {
                    self.state.handle_input(InputIntent::ChangeSortOrder("name".to_string()))
                }
                KeyCode::Char('2') => {
                    self.state.handle_input(InputIntent::ChangeSortOrder("name-desc".to_string()))
                }
                KeyCode::Char('3') => self
                    .state
                    .handle_input(InputIntent::ChangeSortOrder("name-natural".to_string())),
                KeyCode::Char('4') => self
                    .state
                    .handle_input(InputIntent::ChangeSortOrder("name-natural-desc".to_string())),
                KeyCode::Char('5') => {
                    self.state.handle_input(InputIntent::ChangeSortOrder("date".to_string()))
                }
                KeyCode::Char('6') => {
                    self.state.handle_input(InputIntent::ChangeSortOrder("date-desc".to_string()))
                }
                KeyCode::Char('7') => {
                    self.state.handle_input(InputIntent::ChangeSortOrder("size".to_string()))
                }
                KeyCode::Char('8') => {
                    self.state.handle_input(InputIntent::ChangeSortOrder("size-desc".to_string()))
                }
                KeyCode::Char('9') => {
                    self.state.handle_input(InputIntent::ChangeSortOrder("random".to_string()))
                }
                KeyCode::Esc | KeyCode::Char('n') => self.state.handle_input(InputIntent::Cancel),
                _ => {}
            }
            return;
        }
        if self.state.show_search {
            match code {
                KeyCode::Esc => {
                    self.state.handle_input(InputIntent::CancelSearch);
                    self.search_buffer.clear();
                }
                KeyCode::Enter => {
                    self.state.handle_input(InputIntent::SubmitSearch(self.search_buffer.clone()));
                    self.search_buffer.clear();
                }
                KeyCode::Backspace => {
                    self.search_buffer.pop();
                }
                KeyCode::Char(c) => {
                    self.search_buffer.push(c);
                }
                _ => {}
            }
            return;
        }

        // 7. Standard Navigation & Actions
        let intent = match code {
            KeyCode::Char('q') | KeyCode::Esc => Some(InputIntent::Quit),
            KeyCode::Down => Some(InputIntent::NextItem),
            KeyCode::Up => Some(InputIntent::PrevItem),

            // Handle Shift+PageDown for NextGroupByDist
            KeyCode::PageDown => {
                if modifiers.contains(KeyModifiers::SHIFT) {
                    Some(InputIntent::NextGroupByDist)
                } else {
                    Some(InputIntent::PageDown)
                }
            }
            KeyCode::PageUp => {
                if modifiers.contains(KeyModifiers::SHIFT) {
                    Some(InputIntent::PreviousGroupByDist)
                } else {
                    Some(InputIntent::PageUp)
                }
            }
            KeyCode::Tab => Some(InputIntent::NextGroup),
            KeyCode::BackTab => Some(InputIntent::PrevGroup),
            KeyCode::Home => Some(InputIntent::Home),
            KeyCode::End => Some(InputIntent::End),

            KeyCode::Char(' ') => Some(InputIntent::ToggleMark),
            KeyCode::Char('d') | KeyCode::Delete => Some(InputIntent::ExecuteDelete),
            KeyCode::Char('m') => {
                if self.state.move_target.is_some() {
                    Some(InputIntent::MoveMarked)
                } else {
                    self.show_move_input = true;
                    self.move_buffer.clear();
                    None
                }
            }
            KeyCode::Char('r') => {
                // Pre-fill buffer with current filename
                if let Some(path) = self.state.get_current_image_path() {
                    self.rename_buffer =
                        path.file_name().unwrap_or_default().to_string_lossy().to_string();
                }
                // Clear any old completion state
                self.completion_candidates.clear();
                self.completion_index = 0;
                Some(InputIntent::StartRename)
            }
            KeyCode::Char('f') if modifiers.contains(KeyModifiers::CONTROL) => {
                Some(InputIntent::StartSearch)
            }
            KeyCode::F(3) | KeyCode::Char('n') => {
                // 'n' for next is common tui convention
                if modifiers.contains(KeyModifiers::SHIFT) || code == KeyCode::Char('N') {
                    Some(InputIntent::PrevSearchResult)
                } else {
                    Some(InputIntent::NextSearchResult)
                }
            }
            KeyCode::Char('s') => Some(InputIntent::ShowSortSelection),
            KeyCode::Char('h') => Some(InputIntent::ToggleRelativeTime),
            KeyCode::Char('p') => Some(InputIntent::TogglePathVisibility),
            KeyCode::Char('x') => Some(InputIntent::ToggleZoomRelative),
            _ => None,
        };

        if let Some(i) = intent {
            if matches!(i, InputIntent::PageDown | InputIntent::PageUp) {
                self.state.move_page(i == InputIntent::PageDown, self.view_height);
                self.state.selection_changed = true;
            } else {
                self.state.handle_input(i);
            }
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();

        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![
                Constraint::Min(0),
                Constraint::Length(1), // Status Bar
            ])
            .split(area);

        self.view_height = (main_layout[0].height.saturating_sub(2) / 2) as usize;

        // --- Status Bar ---
        let status_widget = if let Some((msg, is_error)) = &self.state.status_message {
            let color = if *is_error { Color::Red } else { Color::Green };
            Paragraph::new(Span::styled(msg, Style::default().fg(color)))
        } else {
            let mode = if self.state.use_trash { "Trash" } else { "Perm" };
            let time_mode = if self.state.show_relative_times { "Rel" } else { "Abs" };
            Paragraph::new(Span::raw(format!(
                "Mode: {} | Time: {} | [Space]: Mark | [d]: Delete | [m]: Move | [r]: Rename | [s]: Sort | [q]: Quit",
                mode, time_mode
            )))
        };

        frame.render_widget(status_widget, main_layout[1]);

        // --- File List ---
        let mut list_items = Vec::new();

        for (g_idx, group) in self.state.groups.iter().enumerate() {
            let info = &self.state.group_infos[g_idx];

            let (header_text, header_color) = match info.status {
                GroupStatus::AllIdentical => (
                    format!("--- Group {} - Bit-identical (hardlinks in magenta) ---", g_idx + 1),
                    Color::Green,
                ),
                GroupStatus::SomeIdentical => {
                    (format!("--- Group {} - Some Identical ---", g_idx + 1), Color::Green)
                }
                GroupStatus::None => (
                    format!("--- Group {} (Dist: {}) ---", g_idx + 1, info.max_dist),
                    Color::Yellow,
                ),
            };

            list_items.push(ListItem::new(Line::from(vec![Span::styled(
                header_text,
                Style::default().fg(header_color).add_modifier(Modifier::BOLD),
            )])));

            let counts = get_bit_identical_counts(group);
            let hardlink_groups = get_hardlink_groups(group);

            for (f_idx, file) in group.iter().enumerate() {
                let is_marked = self.state.marked_for_deletion.contains(&file.path);
                let is_selected =
                    g_idx == self.state.current_group_idx && f_idx == self.state.current_file_idx;
                let is_bit_identical = *counts.get(&file.content_hash).unwrap_or(&0) > 1;
                let is_hardlinked = hardlink_groups.contains_key(&file.unique_file_id);

                let style = if is_selected {
                    Style::default().fg(Color::Blue)
                } else if is_marked {
                    Style::default().fg(Color::Red)
                } else if is_hardlinked {
                    Style::default().fg(Color::Magenta)
                } else if is_bit_identical {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default()
                };

                let marker = if is_marked { "*" } else { " " };
                let marker_style = if is_marked {
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };

                let path_display = format_path_depth(&file.path, self.state.path_display_depth);

                let size_kb = file.size / 1024;

                let time_str = if self.state.show_relative_times {
                    let ts = Timestamp::from_second(file.modified.timestamp())
                        .unwrap()
                        .checked_add(jiff::SignedDuration::from_nanos(
                            file.modified.timestamp_subsec_nanos() as i64,
                        ))
                        .unwrap();
                    format_relative_time(ts)
                } else {
                    file.modified.format("%Y-%m-%d %H:%M:%S").to_string()
                };

                let res_str = if let Some((w, h)) = file.resolution {
                    format!("{}x{}", w, h)
                } else {
                    "???x???".to_string()
                };

                let line1 = Line::from(vec![
                    Span::styled(format!("{} ", marker), marker_style),
                    Span::styled(path_display, style.add_modifier(Modifier::BOLD)),
                ]);

                let line2 = Line::from(vec![
                    Span::raw("  "),
                    Span::styled(format!("{} | {} KB | {}", time_str, size_kb, res_str), style),
                ]);

                list_items.push(ListItem::new(vec![line1, line2]));
            }
        }

        let list = List::new(list_items)
            .block(Block::default().borders(Borders::ALL).title("Duplicates"))
            .highlight_symbol(">> ");

        frame.render_stateful_widget(list, main_layout[0], &mut self.list_state);

        // --- Modals / Popups ---

        // 1. Confirmation Popups
        if self.state.show_confirmation {
            let action = if self.state.use_trash { "trash" } else { "delete" };
            let text = format!(
                "Are you sure you want to {} {} files?\n\n(y) Yes / (n) No",
                action,
                self.state.marked_for_deletion.len()
            );
            render_popup(frame, "Confirm Deletion", &text, 60, 20, Color::Red);
        }

        if self.state.show_delete_immediate_confirmation {
            let path = self.state.get_current_image_path();
            let name = path
                .map(|p| p.file_name().unwrap_or_default().to_string_lossy())
                .unwrap_or_default();
            let action = if self.state.use_trash { "trash" } else { "delete" };
            let text = format!("{} current file?\n{}\n\n(y) Yes / (n) No", action, name);
            render_popup(frame, "Confirm Delete", &text, 60, 20, Color::Red);
        }

        if self.state.show_move_confirmation {
            let target = self
                .state
                .move_target
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or("???".into());
            let text = format!(
                "Move {} files to:\n{}\n\n(y) Yes / (n) No",
                self.state.marked_for_deletion.len(),
                target
            );
            render_popup(frame, "Confirm Move", &text, 60, 20, Color::Cyan);
        }

        // 2. Sort Menu
        if self.state.show_sort_selection {
            let text = "Select Sort Order:\n\n1. Name (A-Z)\n2. Name (Z-A)\n3. Name Natural (A-Z)\n4. Name Natural (Z-A)\n5. Date (Oldest)\n6. Date (Newest)\n7. Size (Smallest)\n8. Size (Largest)\n9. Random\n\n(Esc) Cancel";
            render_popup(frame, "Sort Order", text, 40, 40, Color::Yellow);
        }

        // 3. Rename Input
        if self.state.renaming.is_some() {
            let block = Block::default()
                .title("Rename File")
                .borders(Borders::ALL)
                .style(Style::default().bg(Color::Blue).fg(Color::White));
            let paragraph = Paragraph::new(self.rename_buffer.clone()).block(block);
            let area = centered_rect(60, 10, area);
            frame.render_widget(Clear, area);
            frame.render_widget(paragraph, area);
        }

        // 4. Error Popup
        if let Some(err_text) = &self.state.error_popup {
            render_popup(frame, "ERROR", err_text, 80, 40, Color::Red);
        }

        if self.state.show_search {
            let block = Block::default()
                .title("Find (Regex)")
                .borders(Borders::ALL)
                .style(Style::default().bg(Color::Blue).fg(Color::White));
            let paragraph = Paragraph::new(self.search_buffer.clone()).block(block);
            let area = centered_rect(60, 10, frame.area());
            frame.render_widget(Clear, area);
            frame.render_widget(paragraph, area);
        }
    }
}

// Helper for generic text popups
fn render_popup(
    frame: &mut Frame,
    title: &str,
    text: &str,
    percent_x: u16,
    percent_y: u16,
    border_color: Color,
) {
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .style(Style::default().bg(Color::DarkGray).fg(border_color));
    let paragraph =
        Paragraph::new(text).block(block).wrap(Wrap { trim: true }).alignment(Alignment::Center);
    let area = centered_rect(percent_x, percent_y, frame.area());
    frame.render_widget(Clear, area);
    frame.render_widget(paragraph, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints(vec![
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
