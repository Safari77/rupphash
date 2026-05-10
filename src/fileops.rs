use file_id::FileId;
use filetime::FileTime;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[cfg(unix)]
use std::os::fd::OwnedFd;

// Standard filename limit for most filesystems
const MAX_FILENAME_BYTES: usize = 255;

/// Holds an open handle to the destination directory plus cached metadata.
///
/// On Unix the OwnedFd is kept alive for the entire confirm+move sequence,
/// so all renameat/openat calls resolve against the same inode the user
/// confirmed in the dialog. This closes the TOCTOU window where the path
/// could be swapped between dialog display and the move itself.
///
/// On Windows there is no dirfd, so we just store the path and rely on
/// path-based operations.
pub struct DestinationDir {
    pub path: PathBuf,
    pub mtime: Option<SystemTime>,
    pub fs_type: String,
    #[cfg(unix)]
    pub dir_fd: OwnedFd,
}

impl DestinationDir {
    /// Open the destination directory and capture its metadata.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags};

            // O_DIRECTORY ensures we error out cleanly if path is not a dir,
            // O_CLOEXEC keeps the fd from leaking across exec.
            let dir_fd = rustix::fs::open(
                path,
                OFlags::DIRECTORY | OFlags::RDONLY | OFlags::CLOEXEC,
                Mode::empty(),
            )?;

            // Read mtime through a duplicated fd wrapped as std::fs::File.
            // This avoids poking platform-specific rustix::fs::Stat fields
            // (which differ between linux_raw and libc backends, and use
            // st_mtimespec on macOS). The duplicate is dropped at the end
            // of this scope, leaving dir_fd intact.
            let mtime = {
                let cloned = dir_fd.try_clone()?;
                let f = std::fs::File::from(cloned);
                f.metadata().and_then(|m| m.modified()).ok()
            };

            let fs_type = detect_fs_type(&dir_fd);

            Ok(Self { path: path.to_path_buf(), mtime, fs_type, dir_fd })
        }

        #[cfg(not(unix))]
        {
            let metadata = std::fs::metadata(path)?;
            if !metadata.is_dir() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "destination is not a directory",
                ));
            }
            let mtime = metadata.modified().ok();
            Ok(Self { path: path.to_path_buf(), mtime, fs_type: "unknown".to_string() })
        }
    }

    /// Convert the cached mtime to a `jiff::Timestamp` for UI display.
    /// Returns `None` if the mtime is missing or out of jiff's range.
    pub fn mtime_timestamp(&self) -> Option<jiff::Timestamp> {
        let mtime = self.mtime?;
        let dur = mtime.duration_since(std::time::UNIX_EPOCH).ok()?;
        // Match dialogs.rs / ui.rs existing pattern (second-precision is fine for display).
        jiff::Timestamp::from_second(dur.as_secs() as i64).ok()
    }
}

/// Outcome of moving one source file into a `DestinationDir`.
pub struct MoveResult {
    pub source: PathBuf,
    /// Final destination path (may differ from `dest.path/src.file_name()`
    /// if the filename had to be truncated).
    pub destination: PathBuf,
    pub outcome: std::io::Result<()>,
}

/// Move every file in `sources` into `dest`. The destination directory's
/// open fd is reused for every file (TOCTOU-safe). Per-file results are
/// returned in input order; one failure does not abort the rest.
pub fn move_files_into(dest: &DestinationDir, sources: &[PathBuf]) -> Vec<MoveResult> {
    sources.iter().map(|src| move_one(dest, src)).collect()
}

fn move_one(dest: &DestinationDir, src: &Path) -> MoveResult {
    // Extract the destination filename from the source.
    let Some(dst_name_os) = src.file_name() else {
        return MoveResult {
            source: src.to_path_buf(),
            destination: dest.path.clone(),
            outcome: Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "source path has no filename component",
            )),
        };
    };
    let Some(dst_name) = dst_name_os.to_str() else {
        return MoveResult {
            source: src.to_path_buf(),
            destination: dest.path.join(dst_name_os),
            outcome: Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "source filename is not valid UTF-8",
            )),
        };
    };

    let (final_name, outcome) = try_move_with_retry(dest, src, dst_name);
    MoveResult { source: src.to_path_buf(), destination: dest.path.join(&final_name), outcome }
}

/// Run the move; on ENAMETOOLONG, truncate the filename and retry once.
fn try_move_with_retry(
    dest: &DestinationDir,
    src: &Path,
    dst_name: &str,
) -> (String, std::io::Result<()>) {
    let outcome = try_move(dest, src, dst_name);

    if let Err(ref e) = outcome
        && is_name_too_long(e)
    {
        let truncated = truncate_filename_to_limit(dst_name);
        if truncated != dst_name {
            eprintln!("Filename too long, retrying with: {}", truncated);
            let retry = try_move(dest, src, &truncated);
            return (truncated, retry);
        }
    }

    (dst_name.to_string(), outcome)
}

/// Single move attempt: rename within the same fs, copy+delete across fs
/// boundaries. Always uses NOREPLACE / O_EXCL so we never overwrite.
fn try_move(dest: &DestinationDir, src: &Path, dst_name: &str) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // 1. Try atomic renameat2 with RENAME_NOREPLACE on the kept-open dirfd.
        match try_renameat_noreplace(dest, src, dst_name) {
            Ok(()) => return Ok(()),
            Err(e) => {
                let raw = e.raw_os_error();
                // EEXIST and ENAMETOOLONG are user-meaningful: surface them.
                if raw == Some(libc::EEXIST) || raw == Some(libc::ENAMETOOLONG) {
                    return Err(e);
                }
                // EXDEV (cross-device), ENOSYS (old kernel), EINVAL (FAT/Android
                // rejecting the flag), EPERM, EOPNOTSUPP -> fall through to copy.
                eprintln!(
                    "[DEBUG] renameat2 failed: {} (errno: {:?}). Trying copy fallback...",
                    e, raw
                );
            }
        }

        // 2. Fallback: copy via openat with O_EXCL on the kept-open dirfd, then unlink src.
        copy_move_into(dest, src, dst_name)
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
    {
        // No renameat_with on macOS/BSD; go straight to the openat+O_EXCL fallback,
        // which still preserves no-overwrite semantics via the kept-open dirfd.
        copy_move_into(dest, src, dst_name)
    }

    #[cfg(not(unix))]
    {
        // Windows: hard_link gives us no-overwrite semantics for free.
        let dst_path = dest.path.join(dst_name);
        if let Err(e) = std::fs::hard_link(src, &dst_path) {
            // Cross-volume or unsupported -> copy with create_new + delete.
            eprintln!("[DEBUG] hard_link failed: {}. Trying copy fallback...", e);
            let mut reader = std::fs::File::open(src)?;
            let mut writer =
                std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_path)?;
            std::io::copy(&mut reader, &mut writer)?;
            writer.sync_all()?;
            drop(writer);
            drop(reader);
        }
        std::fs::remove_file(src)?;
        Ok(())
    }
}

/// Atomic-no-replace rename using rustix's `renameat_with` (Linux/Android).
#[cfg(any(target_os = "linux", target_os = "android"))]
fn try_renameat_noreplace(
    dest: &DestinationDir,
    src: &Path,
    dst_name: &str,
) -> std::io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    use std::os::fd::AsFd;

    // Source path resolves against CWD; destination resolves against the
    // kept-open dirfd, so the destination half is TOCTOU-safe.
    renameat_with(CWD, src, dest.dir_fd.as_fd(), dst_name, RenameFlags::NOREPLACE)?;
    Ok(())
}

/// Copy + delete via the kept-open dirfd. Uses O_EXCL so we never overwrite,
/// and restores permissions / timestamps / xattrs on a best-effort basis.
#[cfg(unix)]
fn copy_move_into(dest: &DestinationDir, src: &Path, dst_name: &str) -> std::io::Result<()> {
    use rustix::fs::{Mode, OFlags, openat};
    use std::os::fd::AsFd;
    use xattr::FileExt;

    // Open source for reading.
    let mut reader = std::fs::File::open(src)?;
    let metadata = reader.metadata()?;

    // Create destination via openat on the kept-open dirfd. O_EXCL ensures
    // we never overwrite an existing file. Mode 0o600 is restrictive at
    // creation; the real perms are restored below from the source metadata.
    let owned_fd = openat(
        dest.dir_fd.as_fd(),
        dst_name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )?;
    // Convert rustix OwnedFd to std::fs::File so we can use std::io::copy
    // and other std-friendly APIs (set_permissions, sync_all).
    let mut writer = std::fs::File::from(owned_fd);

    // Copy data.
    std::io::copy(&mut reader, &mut writer)?;

    // Restore permissions on the file handle (best effort).
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(metadata.permissions().mode());
        if let Err(e) = writer.set_permissions(perms) {
            eprintln!(
                "[WARN] Failed to restore permissions on {:?}: {}",
                dest.path.join(dst_name),
                e
            );
        }
    }

    // Restore timestamps via the file handle (avoids path-based TOCTOU).
    let mtime = FileTime::from_last_modification_time(&metadata);
    let atime = FileTime::from_last_access_time(&metadata);
    if let Err(e) = filetime::set_file_handle_times(&writer, Some(atime), Some(mtime)) {
        eprintln!("[WARN] Failed to restore timestamps on {:?}: {}", dest.path.join(dst_name), e);
    }

    // Restore extended attributes (ACLs, SELinux labels, user xattrs) on the
    // freshly created fd via xattr::FileExt, ignoring per-attr errors.
    if let Ok(iter) = xattr::list(src) {
        for name in iter {
            if let Ok(Some(value)) = xattr::get(src, &name) {
                let _ = writer.set_xattr(&name, &value);
            }
        }
    }

    // Fsync before unlinking the source so the new file is durable.
    writer.sync_all()?;

    // Drop handles before removing source (purely tidy; not required).
    drop(writer);
    drop(reader);

    std::fs::remove_file(src)?;
    Ok(())
}

/// Look up the filesystem type via fstatfs on Linux/Android. Other Unix
/// platforms don't expose a portable f_type magic, so we report "unknown".
#[cfg(any(target_os = "linux", target_os = "android"))]
fn detect_fs_type(dir_fd: &OwnedFd) -> String {
    match rustix::fs::fstatfs(dir_fd) {
        Ok(statfs) => {
            let magic = statfs.f_type as u64;
            fs_magic_to_name(magic)
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("unknown (0x{:x})", magic))
        }
        Err(_) => "unknown".to_string(),
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn detect_fs_type(_dir_fd: &OwnedFd) -> String {
    "unknown".to_string()
}

/// Map a Linux fstatfs f_type magic number to a human-readable name.
/// Values from <linux/magic.h>.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn fs_magic_to_name(magic: u64) -> Option<&'static str> {
    Some(match magic {
        0xEF53 => "ext2/3/4",
        0x9123683E => "btrfs",
        0x58465342 => "xfs",
        0xF2F52010 => "f2fs",
        0x01021994 => "tmpfs",
        0x4D44 => "vfat/msdos",
        0x5346544E => "ntfs",
        0x7366746E => "ntfs3",
        0x6969 => "nfs",
        0xFF534D42 => "cifs/smb",
        0xFE534D42 => "smb2",
        0x517B => "smb",
        0x65735546 => "fuse",
        0x73717368 => "squashfs",
        0x794C7630 => "overlayfs",
        0xCA451A4E => "bcachefs",
        0x2FC12FC1 => "zfs",
        0x5346414F => "afs",
        0xE0F5E1E2 => "erofs",
        0x01021997 => "9p",
        0x3434 => "nilfs",
        0x52654973 => "reiserfs",
        0x858458F6 => "ramfs",
        0xF15F => "ecryptfs",
        0x9660 => "isofs",
        0x72B6 => "jffs2",
        0x28CD3D45 => "cramfs",
        0x9FA0 => "proc",
        0x62656572 => "sysfs",
        0x1373 => "devtmpfs",
        0x1CD1 => "devpts",
        0x63677270 => "cgroup2",
        0x27E0EB => "cgroup",
        0x73636673 => "securityfs",
        0x64626720 => "debugfs",
        0x74726163 => "tracefs",
        0x4244 => "hfs",
        0x6E736673 => "nsfs",
        0xCAFE4A11 => "bpf",
        _ => return None,
    })
}

/// Helper to detect if an error is "Filename too long"
fn is_name_too_long(err: &std::io::Error) -> bool {
    err.raw_os_error() == Some(libc::ENAMETOOLONG)
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
