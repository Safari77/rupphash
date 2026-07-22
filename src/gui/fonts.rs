//! Font discovery and selection.
//!
//! Faces are chosen by inspecting the font file with skrifa rather than by
//! hardcoded collection indices — those shift between font releases and differ
//! per weight file.

use eframe::egui;
use skrifa::instance::{LocationRef, Size};
use skrifa::raw::{FileRef, TableProvider};
use skrifa::string::StringId;
use skrifa::{FontRef, MetadataProvider};
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

/// Which egui family we are trying to fill.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FontRole {
    Proportional,
    Monospace,
}

/// Everything we need to know about one face inside a font file.
#[derive(Clone, Debug)]
pub struct FaceInfo {
    pub index: u32,
    pub name: String,
    pub monospace: bool,
    pub cjk: bool,
}

/// Orthography preference, best first. Decides which Han shapes you get for
/// codepoints shared between Japanese and Chinese (直, 骨, 令, ...).
pub const DEFAULT_ORTHOGRAPHY: &[&str] = &["j", "sc", "tc", "hc", "k", "cl"];

/// Number of fonts inside `data`: 1 for a bare font file, N for a .ttc.
fn fonts_in_file(data: &[u8]) -> u32 {
    match FileRef::new(data) {
        Ok(FileRef::Font(_)) => 1,
        Ok(FileRef::Collection(c)) => c.len(),
        Err(_) => 0,
    }
}

/// Borrow the face at `index`, handling bare fonts and collections alike.
fn font_at(data: &[u8], index: u32) -> Option<FontRef<'_>> {
    match FileRef::new(data).ok()? {
        FileRef::Font(f) => (index == 0).then_some(f),
        FileRef::Collection(c) => c.get(index).ok(),
    }
}

fn full_name(font: &FontRef) -> String {
    font.localized_strings(StringId::FULL_NAME)
        .english_or_first()
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// post.isFixedPitch, with an advance-width fallback for fonts that lie.
/// Advances are in font units here (unscaled), so the epsilon is 1 unit.
fn is_monospace(font: &FontRef) -> bool {
    if font.post().map(|p| p.is_fixed_pitch() != 0).unwrap_or(false) {
        return true;
    }
    let charmap = font.charmap();
    let metrics = font.glyph_metrics(Size::unscaled(), LocationRef::default());
    let (Some(i), Some(m)) = (charmap.map('i'), charmap.map('M')) else {
        return false;
    };
    match (metrics.advance_width(i), metrics.advance_width(m)) {
        (Some(a), Some(b)) => (a - b).abs() < 1.0,
        _ => false,
    }
}

/// Kana plus a basic ideograph — enough to rule out Latin-only faces.
fn has_cjk(font: &FontRef) -> bool {
    let cm = font.charmap();
    cm.map('あ').is_some() && cm.map('一').is_some()
}

pub fn enumerate_faces(data: &[u8]) -> Vec<FaceInfo> {
    (0..fonts_in_file(data))
        .filter_map(|index| {
            let font = font_at(data, index)?;
            Some(FaceInfo {
                index,
                name: full_name(&font),
                monospace: is_monospace(&font),
                cjk: has_cjk(&font),
            })
        })
        .collect()
}

/// Higher is better. `None` means "never pick this face".
fn score_face(face: &FaceInfo, role: FontRole, orthography: &[&str]) -> Option<i32> {
    let tokens: Vec<String> =
        face.name.split_whitespace().map(|t| t.to_ascii_lowercase()).collect();
    let has = |t: &str| tokens.iter().any(|x| x == t);

    // Never use a slanted face as a base UI font.
    if has("italic") || has("oblique") {
        return None;
    }

    let mut score = 0i32;

    if has("sarasa") {
        // Sarasa ships every sub-family in one .ttc, so the sub-family word is a
        // far better signal than post.isFixedPitch — which is set on faces whose
        // Latin is proportional, and unset on some that are usable as monospace.
        score += match role {
            FontRole::Monospace => {
                if has("term") {
                    50 // terminal line metrics, CJK exactly 2x Latin advance
                } else if has("fixed") {
                    45
                } else if has("mono") {
                    40
                } else {
                    return None; // Gothic / UI are not monospace
                }
            }
            FontRole::Proportional => {
                if has("ui") {
                    50 // tighter line height, designed for UI chrome
                } else if has("gothic") {
                    45
                } else {
                    10 // a mono face works, it just isn't the intent
                }
            }
        };
        if has("slab") {
            score -= 8; // stylistic variant, only if nothing else fits
        }
        // Earlier entries in the preference list win.
        if let Some(pos) = orthography.iter().position(|o| has(&o.to_ascii_lowercase())) {
            score += 30 - (pos as i32 * 4);
        }
    } else {
        // Generic font: trust the tables, nudge with naming conventions.
        match role {
            FontRole::Monospace => {
                score += if face.monospace { 40 } else { -20 };
                if has("mono") || has("term") || has("fixed") {
                    score += 10;
                }
            }
            FontRole::Proportional => {
                if face.monospace {
                    score -= 15;
                }
            }
        }
        if has("regular") || has("book") {
            score += 5;
        }
    }

    if face.cjk {
        score += 15;
    }

    Some(score)
}

pub fn pick_face(data: &[u8], role: FontRole, orthography: &[&str]) -> Option<FaceInfo> {
    let faces = enumerate_faces(data);
    if faces.is_empty() {
        return None;
    }
    // A single-face file is the only choice there is — don't second-guess it.
    if faces.len() == 1 {
        return faces.into_iter().next();
    }
    faces
        .into_iter()
        .filter_map(|f| score_face(&f, role, orthography).map(|s| (s, f)))
        // Reverse(index) so ties resolve to the lowest index.
        .max_by_key(|(s, f)| (*s, std::cmp::Reverse(f.index)))
        .map(|(_, f)| f)
}

pub fn install_font_file(
    fonts: &mut egui::FontDefinitions,
    label: &str,
    data: &'static [u8],
    orthography: &[&str],
) {
    install_role(fonts, label, data, FontRole::Proportional, orthography);
    install_role(fonts, label, data, FontRole::Monospace, orthography);
}

/// Read and leak a font file, once per path.
///
/// Fonts live for the whole process, so leaking is fine — but font_ui and
/// font_monospace usually point at the same .ttc, and Sarasa is ~40 MiB.
/// Caching keeps that to one copy shared by both families.
pub fn load_font_data(path: &Path) -> Option<&'static [u8]> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, &'static [u8]>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(|| Mutex::new(HashMap::new())).lock().ok()?;

    if let Some(data) = cache.get(path) {
        return Some(*data);
    }
    match std::fs::read(path) {
        Ok(data) => {
            let leaked: &'static [u8] = Box::leak(data.into_boxed_slice());
            cache.insert(path.to_path_buf(), leaked);
            Some(leaked)
        }
        Err(e) => {
            eprintln!("[FONT] cannot read {}: {e}", path.display());
            None
        }
    }
}

/// Install the best face for one role from one font file.
pub fn install_role(
    fonts: &mut egui::FontDefinitions,
    label: &str,
    data: &'static [u8],
    role: FontRole,
    orthography: &[&str],
) {
    let family = match role {
        FontRole::Proportional => egui::FontFamily::Proportional,
        FontRole::Monospace => egui::FontFamily::Monospace,
    };
    let Some(face) = pick_face(data, role, orthography) else {
        eprintln!("[FONT] {label}: no usable face for {role:?}");
        return;
    };
    eprintln!(
        "[FONT] {family:?} <- {label} #{} {:?} (mono={} cjk={})",
        face.index, face.name, face.monospace, face.cjk
    );

    // Distinct key per face: one file can feed both families, and a shared key
    // would make the second insert clobber the first.
    let key = format!("{label}#{}", face.index);
    let mut fd = egui::FontData::from_static(data);
    fd.index = face.index;
    fonts.font_data.insert(key.clone(), Arc::new(fd));

    // Front of the chain, keeping the rest: the defaults end with the emoji
    // fonts your title bar and folder icons depend on.
    fonts.families.entry(family).or_default().insert(0, key);
}
