use file_id::FileId;
use filetime::FileTime;
use std::path::{Path, PathBuf};

// Standard filename limit for most filesystems
const MAX_FILENAME_BYTES: usize = 255;

/// Low-level helper: Rename a file atomically without overwriting the destination.
/// Includes fallback for Android/FAT filesystems where renameat2 is not supported.
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    // Android is a "Unix" but not "Linux" in Rust `cfg` terms (target_os = "android").
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // 1. Try atomic renameat2 via nix (Linux Only)
        #[cfg(target_os = "linux")]
        {
            use nix::fcntl::{RenameFlags, renameat2};
            use std::os::fd::BorrowedFd;

            // Safety: AT_FDCWD is a constant valid fd for CWD
            let cwd = unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) };

            match renameat2(cwd, from, cwd, to, RenameFlags::RENAME_NOREPLACE) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    // Optimization: If it's a cross-device link, return immediately.
                    // Fallbacks (link/rename) will definitely fail with EXDEV too.
                    if e == nix::errno::Errno::EXDEV {
                        return Err(std::io::Error::from_raw_os_error(libc::EXDEV));
                    }
                    // If the OS explicitly says "File Exists", respect it immediately.
                    if e == nix::errno::Errno::EEXIST {
                        return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
                    }
                    eprintln!(
                        "[DEBUG] renameat2 failed: {} (errno: {}). Checking fallback...",
                        e, e as i32
                    );
                }
            }
        }

        // 2. Fallback: Hard Link + Unlink (POSIX atomic standard)
        // This fails on Android /sdcard (FAT/Emulated)
        eprintln!("[DEBUG] Attempting fallback: hard link + unlink");
        if std::fs::hard_link(from, to).is_ok() {
            let _ = std::fs::remove_file(from);
            return Ok(());
        }

        // 3. Last Resort
        eprintln!("[DEBUG] Attempting last resort: rename (if dst missing)");
        if to.exists() {
            return Err(std::io::Error::from_raw_os_error(libc::EEXIST));
        }
        std::fs::rename(from, to)
    }

    #[cfg(all(unix, not(target_os = "linux"), not(target_os = "android")))]
    {
        eprintln!("[DEBUG] rename_noreplace: using hard_link fallback (non-linux)");
        match std::fs::hard_link(from, to) {
            Ok(()) => {
                std::fs::remove_file(from)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    #[cfg(windows)]
    {
        eprintln!("[DEBUG] rename_noreplace: using hard_link fallback (windows)");
        match std::fs::hard_link(from, to) {
            Ok(()) => {
                std::fs::remove_file(from)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

/// Helper: Robustly Copy + Delete when Cross-Device Move is required.
/// Uses O_EXCL to ensure we NEVER overwrite an existing file.
/// Restores Metadata: Permissions, Timestamps, and Extended Attributes (ACLs/SELinux).
fn atomic_copy_move(src: &Path, dst: &Path) -> std::io::Result<()> {
    // Open Source & Read Metadata
    let mut reader = std::fs::File::open(src)?;
    let metadata = reader.metadata()?;

    // Open Destination with O_EXCL (Fail if exists)
    let mut writer = std::fs::OpenOptions::new().write(true).create_new(true).open(dst)?;

    // Copy Data
    std::io::copy(&mut reader, &mut writer)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Restore Permissions (Best Effort)
        let mut perms = metadata.permissions();
        perms = std::fs::Permissions::from_mode(perms.mode());
        if let Err(e) = std::fs::set_permissions(dst, perms) {
            eprintln!("[WARN] Failed to restore permissions on {:?}: {}", dst, e);
        }
    }

    // Restore Timestamps (mtime, atime)
    let mtime = FileTime::from_last_modification_time(&metadata);
    let atime = FileTime::from_last_access_time(&metadata);
    if let Err(e) = filetime::set_file_times(dst, atime, mtime) {
        eprintln!("[WARN] Failed to restore timestamps on {:?}: {}", dst, e);
    }

    // Restore Extended Attributes (ACLs, SELinux Labels, User xattrs), ignore errors.
    #[cfg(unix)]
    {
        if let Ok(iter) = xattr::list(src) {
            for name in iter {
                // Try to read the attribute value
                if let Ok(Some(value)) = xattr::get(src, &name) {
                    // Try to set it on the destination.
                    let _ = xattr::set(dst, &name, &value);
                }
            }
        }
    }

    // Fsync
    writer.sync_all()?;

    // Cleanup
    drop(writer);
    drop(reader);

    std::fs::remove_file(src)?;
    Ok(())
}

/// Helper to detect if an error is "Filename too long"
fn is_name_too_long(err: &std::io::Error) -> bool {
    err.raw_os_error() == Some(libc::ENAMETOOLONG)
}

/// Helper to detect if an error is EXDEV (Cross-device link)
fn is_cross_device(err: &std::io::Error) -> bool {
    err.raw_os_error() == Some(libc::EXDEV) || err.kind() == std::io::ErrorKind::CrossesDevices
}

/// High-level helper: Handles atomic moves, cross-device copying,
/// and "Filename Too Long" truncation logic automatically.
pub fn perform_atomic_move(temp_path: &Path, target_path: &Path) -> std::io::Result<PathBuf> {
    // Helper closure that tries rename_noreplace, and falls back to atomic_copy_move on EXDEV
    let try_move = |src: &Path, dst: &Path| -> std::io::Result<()> {
        match rename_noreplace(src, dst) {
            Err(e) if is_cross_device(&e) => {
                // Cross-device logic: Copy, Sync, Delete
                atomic_copy_move(src, dst)
            }
            other => other,
        }
    };

    // Attempt 1: Try the move
    match try_move(temp_path, target_path) {
        Ok(_) => Ok(target_path.to_path_buf()),

        Err(e)
            if e.kind() == std::io::ErrorKind::AlreadyExists
                || e.raw_os_error() == Some(libc::EEXIST) =>
        {
            Err(e)
        }

        // Handle "Filename Too Long" logic
        Err(e) if is_name_too_long(&e) => {
            let truncated = resolve_output_path(target_path)?;
            // Safety check: if truncation didn't actually shorten it, stop
            if truncated == target_path {
                return Err(e);
            }
            eprintln!("Filename too long, retrying with: {}", truncated.display());

            // Attempt 2: Try with truncated name
            match try_move(temp_path, &truncated) {
                Ok(_) => Ok(truncated),
                Err(e)
                    if e.kind() == std::io::ErrorKind::AlreadyExists
                        || e.raw_os_error() == Some(libc::EEXIST) =>
                {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("Truncated filename '{}' already exists", truncated.display()),
                    ))
                }
                Err(e) => Err(e),
            }
        }

        // Catch-all
        Err(e) => Err(e),
    }
}

/// Resolves the path, truncating the filename if it exceeds limits.
fn resolve_output_path(original_path: &Path) -> std::io::Result<PathBuf> {
    let filename = original_path.file_name().and_then(|s| s.to_str()).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename in path")
    })?;

    if filename.len() <= MAX_FILENAME_BYTES {
        return Ok(original_path.to_path_buf());
    }

    let truncated = truncate_filename_to_limit(filename);
    let parent = original_path.parent().unwrap_or(Path::new("."));
    Ok(parent.join(&truncated))
}

/// Truncate a filename to fit within MAX_FILENAME_BYTES, preserving extension if possible.
fn truncate_filename_to_limit(filename: &str) -> String {
    if filename.len() <= MAX_FILENAME_BYTES {
        return filename.to_string();
    }

    let (base, ext) = if let Some(dot_pos) = filename.rfind('.') {
        if dot_pos > 0 && filename.len() - dot_pos <= 20 {
            (&filename[..dot_pos], &filename[dot_pos..])
        } else {
            (filename, "")
        }
    } else {
        (filename, "")
    };

    let ext_bytes = ext.len();
    let max_base_bytes = MAX_FILENAME_BYTES.saturating_sub(ext_bytes);

    if max_base_bytes == 0 {
        return truncate_str_to_byte_limit(filename, MAX_FILENAME_BYTES).to_string();
    }

    let truncated_base = truncate_str_to_byte_limit(base, max_base_bytes);
    format!("{}{}", truncated_base, ext)
}

/// Truncate a string to fit within a maximum byte limit, respecting UTF-8 boundaries.
fn truncate_str_to_byte_limit(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }

    let mut end = 0;
    for (idx, c) in s.char_indices() {
        let next_end = idx + c.len_utf8();
        if next_end <= max_bytes {
            end = next_end;
        } else {
            break;
        }
    }

    &s[..end]
}

pub fn get_file_key(path: &Path) -> Option<u128> {
    // 1. Fallback for non-Unix/Windows: Return truncated blake3 of path
    #[cfg(not(any(unix, windows)))]
    {
        let hash = blake3::hash(path.to_string_lossy().as_bytes());
        // Take the first 16 bytes (128 bits)
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash.as_bytes()[0..16]);
        return Some(u128::from_le_bytes(bytes));
    }

    // 2. Standard Logic for Unix/Windows
    #[cfg(any(unix, windows))]
    {
        let id = {
            #[cfg(unix)]
            {
                file_id::get_file_id(path).ok()?
            }
            #[cfg(windows)]
            {
                file_id::get_high_res_file_id(path).ok()?
            }
        };

        Some(match id {
            FileId::Inode { device_id, inode_number } => {
                ((device_id as u128) << 64) | (inode_number as u128)
            }
            FileId::LowRes { volume_serial_number, file_index } => {
                ((volume_serial_number as u128) << 64) | (file_index as u128)
            }
            FileId::HighRes { volume_serial_number, file_id } => {
                file_id ^ ((volume_serial_number as u128) << 64)
            }
        })
    }
}
