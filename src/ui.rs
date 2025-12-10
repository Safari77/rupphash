use std::io::{self, Stdout};
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Clear, Wrap},
};
use jiff::Timestamp;

// Removed unused import: crate::FileMetadata
use crate::GroupStatus;
use crate::format_relative_time;
use crate::state::{AppState, InputIntent, format_path_depth, get_bit_identical_counts};

pub struct TuiApp {
    state: AppState,
    list_state: ListState,
    view_height: usize,
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
                    && key.kind == KeyEventKind::Press {
                        self.handle_key(key.code);
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
        // Calculate absolute index for Ratatui list
        // Each group has: 1 header item + N file items
        // Note: Each file item is 1 ListItem (even though it has 2 lines internally)
        let mut abs_idx = 0;
        for i in 0..self.state.current_group_idx {
            abs_idx += 1 + self.state.groups[i].len(); // 1 header + file count
        }
        abs_idx += 1 + self.state.current_file_idx; // 1 for current group header + file index
        self.list_state.select(Some(abs_idx));
    }

    fn handle_key(&mut self, key: KeyCode) {
        // Error popup - dismiss on any key
        if self.state.error_popup.is_some() {
            self.state.handle_input(InputIntent::Cancel);
            return;
        }

        // Modal / Confirmation Override
        if self.state.show_confirmation {
             match key {
                KeyCode::Char('y') | KeyCode::Char('Y') => self.state.handle_input(InputIntent::ConfirmDelete),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => self.state.handle_input(InputIntent::Cancel),
                _ => {}
             }
             return;
        }

        // Renaming state - cancel only (TUI doesn't support rename UI)
        if self.state.renaming.is_some() {
            if matches!(key, KeyCode::Esc) {
                self.state.handle_input(InputIntent::Cancel);
            }
            return;
        }

        // Standard Mapping
        let intent = match key {
            KeyCode::Char('q') | KeyCode::Esc => Some(InputIntent::Quit),
            KeyCode::Down => Some(InputIntent::NextItem),
            KeyCode::Up => Some(InputIntent::PrevItem),
            KeyCode::PageDown => Some(InputIntent::PageDown),
            KeyCode::PageUp => Some(InputIntent::PageUp),
            KeyCode::Tab => Some(InputIntent::NextGroup),
            KeyCode::BackTab => Some(InputIntent::PrevGroup),
            KeyCode::Home => Some(InputIntent::Home),
            KeyCode::End => Some(InputIntent::End),
            KeyCode::Char(' ') => Some(InputIntent::ToggleMark),
            KeyCode::Char('d') => Some(InputIntent::ExecuteDelete),
            KeyCode::Char('h') => Some(InputIntent::ToggleRelativeTime),
            KeyCode::Char('p') => Some(InputIntent::TogglePathVisibility),
            KeyCode::Char('x') => Some(InputIntent::ToggleZoomRelative), // Added for consistency (affects state even if TUI doesn't show images)
            _ => None,
        };

        if let Some(i) = intent {
            // Special handling for paging which needs view dimension from renderer
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

        // Update view height for paging
        // Each file item takes 2 lines, headers take 1 line, plus 2 for borders
        // For accurate paging, we estimate visible items: (height - 2 borders) / 2 lines per item
        // This is approximate since headers only take 1 line
        self.view_height = (main_layout[0].height.saturating_sub(2) / 2) as usize;

        // --- Status Bar ---
        let status_widget = if let Some((msg, is_error)) = &self.state.status_message {
             let color = if *is_error { Color::Red } else { Color::Green };
             Paragraph::new(Span::styled(msg, Style::default().fg(color)))
        } else {
             let mode = if self.state.use_trash { "Trash" } else { "Perm" };
             let time_mode = if self.state.show_relative_times { "Rel" } else { "Abs" };
             Paragraph::new(Span::raw(format!("Mode: {} | Time: {} | [Space]: Mark | [d]: Delete | [h]: Time | [p]: Path | [Tab]: Groups | [q]: Quit", mode, time_mode)))
        };

        frame.render_widget(status_widget, main_layout[1]);

        // --- File List ---
        let mut list_items = Vec::new();

        for (g_idx, group) in self.state.groups.iter().enumerate() {
            let info = &self.state.group_infos[g_idx];

            let (header_text, header_color) = match info.status {
                GroupStatus::AllIdentical => (
                    format!("--- Group {} - Bit-identical ---", g_idx + 1),
                    Color::Green
                ),
                GroupStatus::SomeIdentical => (
                    format!("--- Group {} - Some Identical ---", g_idx + 1),
                    Color::Green
                ),
                GroupStatus::None => (
                    format!("--- Group {} (Dist: {}) ---", g_idx + 1, info.max_dist),
                    Color::Yellow
                ),
            };

            list_items.push(ListItem::new(Line::from(vec![
                Span::styled(header_text,
                    Style::default().fg(header_color).add_modifier(Modifier::BOLD)),
            ])));

            let counts = get_bit_identical_counts(group);

            for (f_idx, file) in group.iter().enumerate() {
                let is_marked = self.state.marked_for_deletion.contains(&file.path);
                let is_selected = g_idx == self.state.current_group_idx && f_idx == self.state.current_file_idx;
                let is_bit_identical = *counts.get(&file.content_hash).unwrap_or(&0) > 1;

                let style = if is_selected {
                     Style::default().fg(Color::Blue)
                } else if is_marked {
                     Style::default().fg(Color::Red)
                } else if is_bit_identical {
                     Style::default().fg(Color::Green)
                } else {
                     Style::default()
                };

                let marker = if is_marked { "*" } else { " " };
                let marker_style = if is_marked { Style::default().fg(Color::Red).add_modifier(Modifier::BOLD) } else { Style::default() };

                let path_display = format_path_depth(&file.path, self.state.path_display_depth);

                let size_kb = file.size / 1024;

                let time_str = if self.state.show_relative_times {
                     let ts = Timestamp::from_second(file.modified.timestamp()).unwrap()
                        .checked_add(jiff::SignedDuration::from_nanos(file.modified.timestamp_subsec_nanos() as i64)).unwrap();
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

        // --- Popups ---
        if self.state.show_confirmation {
            let block = Block::default().title("Confirmation").borders(Borders::ALL).style(Style::default().bg(Color::DarkGray));
            let action = if self.state.use_trash { "trash" } else { "delete" };
            let text = format!("Are you sure you want to {} {} files?\n\n(y) Yes / (n) No", action, self.state.marked_for_deletion.len());
            let paragraph = Paragraph::new(text).block(block).alignment(Alignment::Center);
            let area = centered_rect(60, 20, area);
            frame.render_widget(Clear, area);
            frame.render_widget(paragraph, area);
        }

        if let Some(err_text) = &self.state.error_popup {
            let block = Block::default().title("ERROR").borders(Borders::ALL).style(Style::default().bg(Color::Red).fg(Color::White));
            let paragraph = Paragraph::new(err_text.clone()).block(block).wrap(Wrap { trim: true }).alignment(Alignment::Left);
            let area = centered_rect(80, 40, area);
            frame.render_widget(Clear, area);
            frame.render_widget(paragraph, area);
        }
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default().direction(Direction::Vertical).constraints(vec![
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ]).split(r);
    Layout::default().direction(Direction::Horizontal).constraints(vec![
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ]).split(popup_layout[1])[1]
}
