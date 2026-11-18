#[cfg(unix)] use std::os::fd::OwnedFd;
use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    time::SystemTime,
};

use file_id::FileId;

// Standard filename limit for most filesystems
const MAX_FILENAME_BYTES: usize = 255;

/// Holds an open handle to the destination directory plus cached metadata.
///
/// On Unix the `OwnedFd` is kept alive for the entire confirm+move sequence,
/// so all `renameat`/`openat` calls resolve against the same inode the user
/// confirmed in the dialog. This closes the TOCTOU window where the path
/// could be swapped between dialog display and the move itself.
///
/// On Windows there is no dirfd, so we just store the path and rely on
/// path-based operations.
#[derive(Debug)]
pub struct DestinationDir {
    pub path: PathBuf,
    pub mtime: Option<SystemTime>,
    pub fs_type: String,
    #[cfg(unix)]
    pub dir_fd: OwnedFd,
}

impl DestinationDir {
    /// Open the destination directory and capture its metadata.
    ///
    /// # Errors
    /// Returns [`std::io::Error`] if `path` cannot be opened or is not a directory.
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
                    std::io::ErrorKind::NotADirectory,
                    "destination is not a directory",
                ));
            }
            let mtime = metadata.modified().ok();
            Ok(Self { path: path.to_path_buf(), mtime, fs_type: "unknown".to_string() })
        }
    }

    /// Convert the cached mtime to a [`jiff::Timestamp`] for UI display.
    /// Returns `None` if the mtime is missing or out of jiff's range.
    #[must_use]
    pub fn mtime_timestamp(&self) -> Option<jiff::Timestamp> {
        let mtime = self.mtime?;
        jiff::Timestamp::try_from(mtime).ok()
    }
}

/// Outcome of moving one source file into a [`DestinationDir`].
#[derive(Debug)]
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
#[must_use]
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

    // Names stay as OsStr end to end: rustix's openat/renameat_with take any
    // Arg, so there is no reason to reject the non-UTF-8 names that turn up on
    // media copied from other systems.
    let (final_name, outcome) = try_move_with_retry(dest, src, dst_name_os);
    MoveResult { source: src.to_path_buf(), destination: dest.path.join(&final_name), outcome }
}

/// Run the move; on ENAMETOOLONG, truncate the filename and retry once.
fn try_move_with_retry(
    dest: &DestinationDir,
    src: &Path,
    dst_name: &OsStr,
) -> (OsString, std::io::Result<()>) {
    let outcome = try_move(dest, src, dst_name);

    if let Err(ref e) = outcome
        && is_name_too_long(e)
        && let Some(name_str) = dst_name.to_str()
    {
        let truncated = truncate_filename_to_limit(name_str);
        if truncated != name_str {
            eprintln!("Filename too long, retrying with: {truncated}");
            let retry = try_move(dest, src, OsStr::new(&truncated));
            return (OsString::from(truncated), retry);
        }
    }
    (dst_name.to_os_string(), outcome)
}

/// Single move attempt: rename within the same fs, copy+delete across fs
/// boundaries. Always uses NOREPLACE / `O_EXCL` so we never overwrite.
fn try_move(dest: &DestinationDir, src: &Path, dst_name: &OsStr) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // 1. Try atomic renameat2 with RENAME_NOREPLACE on the kept-open dirfd.
        match try_renameat_noreplace(dest, src, dst_name) {
            Ok(()) => return Ok(()),
            Err(e) => {
                // AlreadyExists and InvalidFilename (ENAMETOOLONG) are user-meaningful;
                // only CrossesDevices, Unsupported, or InvalidInput (FAT/Android rejecting
                // RENAME_NOREPLACE) should fall through to copy+unlink. Other source/permission
                // errors (ENOENT, EACCES, EROFS) are returned immediately.
                let raw = e.raw_os_error();
                let should_fallback = matches!(
                    raw,
                    Some(
                        libc::EXDEV | libc::ENOSYS | libc::EINVAL | libc::EPERM | libc::EOPNOTSUPP
                    )
                );
                if !should_fallback {
                    return Err(e);
                }
                eprintln!(
                    "[DEBUG] renameat2 failed: {e} (errno: {raw:?}). Trying copy fallback..."
                );
            },
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
            if e.kind() == std::io::ErrorKind::AlreadyExists || is_name_too_long(&e) {
                return Err(e);
            }
            // Cross-volume or unsupported -> copy with create_new + delete.
            eprintln!("[DEBUG] hard_link failed: {e}. Trying copy fallback...");
            let reader = std::fs::File::open(src)?;
            let writer =
                std::fs::OpenOptions::new().write(true).create_new(true).open(&dst_path)?;
            if let Err(copy_err) = copy_windows_contents(&dst_path, reader, writer) {
                let _ = std::fs::remove_file(&dst_path);
                return Err(copy_err);
            }
        }
        std::fs::remove_file(src)?;
        Ok(())
    }
}

/// Restore access and modification timestamps via the open file handle
/// (avoids path-based TOCTOU). Best effort: logs a warning on failure.
fn restore_timestamps(writer: &std::fs::File, metadata: &std::fs::Metadata, dst_path: &Path) {
    let mut times = std::fs::FileTimes::new();
    let mut has_any = false;
    if let Ok(atime) = metadata.accessed() {
        times = times.set_accessed(atime);
        has_any = true;
    }
    if let Ok(mtime) = metadata.modified() {
        times = times.set_modified(mtime);
        has_any = true;
    }
    if has_any && let Err(e) = writer.set_times(times) {
        eprintln!("[WARN] Failed to restore timestamps on {dst_path:?}: {e}");
    }
}

#[cfg(not(unix))]
fn copy_windows_contents(
    dst_path: &Path,
    mut reader: std::fs::File,
    mut writer: std::fs::File,
) -> std::io::Result<()> {
    let metadata = reader.metadata()?;
    std::io::copy(&mut reader, &mut writer)?;
    restore_timestamps(&writer, &metadata, dst_path);
    writer.sync_all()?;
    Ok(())
}

/// Atomic-no-replace rename using rustix's `renameat_with` (Linux/Android).
#[cfg(any(target_os = "linux", target_os = "android"))]
fn try_renameat_noreplace(
    dest: &DestinationDir,
    src: &Path,
    dst_name: &OsStr,
) -> std::io::Result<()> {
    use std::os::fd::AsFd;

    use rustix::fs::{CWD, RenameFlags, renameat_with};

    // Source path resolves against CWD; destination resolves against the
    // kept-open dirfd, so the destination half is TOCTOU-safe.
    renameat_with(CWD, src, dest.dir_fd.as_fd(), dst_name, RenameFlags::NOREPLACE)?;
    Ok(())
}

/// Copy + delete via the kept-open dirfd. Uses `O_EXCL` so we never overwrite,
/// and restores permissions / timestamps / xattrs on a best-effort basis.
#[cfg(unix)]
fn copy_move_into(dest: &DestinationDir, src: &Path, dst_name: &OsStr) -> std::io::Result<()> {
    use std::os::fd::AsFd;

    use rustix::fs::{AtFlags, Mode, OFlags, openat, unlinkat};

    // Open source for reading and ensure it is a regular file (never follow or copy a directory/FIFO).
    let reader = std::fs::File::open(src)?;
    let src_meta = reader.metadata()?;
    if !src_meta.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "source is not a regular file",
        ));
    }

    // Create destination via openat on the kept-open dirfd. O_EXCL ensures
    // we never overwrite an existing file. Mode 0o600 is restrictive at
    // creation; the real perms are restored below from the source metadata.
    // Everything that can fail *after* this point must unlink what we just
    // created: a half-written file (ENOSPC, EIO) would otherwise stay behind
    // and make every retry fail with EEXIST.
    let owned_fd = openat(
        dest.dir_fd.as_fd(),
        dst_name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )?;
    // Convert rustix OwnedFd to std::fs::File so we can use std::io::copy
    // and other std-friendly APIs (set_permissions, sync_all).
    let writer = std::fs::File::from(owned_fd);

    if let Err(e) = copy_contents_and_metadata(dest, dst_name, &src_meta, reader, writer) {
        if let Err(unlink_err) = unlinkat(dest.dir_fd.as_fd(), dst_name, AtFlags::empty()) {
            eprintln!(
                "[WARN] Failed to remove partial file {:?}: {unlink_err}",
                dest.path.join(dst_name),
            );
        }
        return Err(e);
    }

    // NOTE: if this fails the copy is already durable, so we keep it and report
    // rather than unwinding — the caller sees the error and both copies exist.
    std::fs::remove_file(src)?;
    Ok(())
}

/// Everything between "destination created" and "source removed". Split out so
/// `copy_move_into` can unlink the destination on any failure in here.
#[cfg(unix)]
fn copy_contents_and_metadata(
    dest: &DestinationDir,
    dst_name: &OsStr,
    metadata: &std::fs::Metadata,
    mut reader: std::fs::File,
    mut writer: std::fs::File,
) -> std::io::Result<()> {
    use xattr::FileExt;

    // Copy data.
    std::io::copy(&mut reader, &mut writer)?;

    // Restore extended attributes (ACLs, SELinux labels, user xattrs) on the
    // freshly created fd via xattr::FileExt before tightening permissions or
    // setting final timestamps, since writing security/ACL xattrs can alter
    // file mode bits or update ctime/mtime on some filesystems.
    //
    // Read through the open source handle rather than by path.
    if let Ok(names) = reader.list_xattr() {
        for name in names {
            if let Ok(Some(value)) = reader.get_xattr(&name) {
                let _ = writer.set_xattr(&name, &value);
            }
        }
    }

    // Restore permissions on the file handle (best effort), masking out
    // setuid/setgid/sticky bits (0o7777 -> 0o0777) for safety.
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o0777;
        let perms = std::fs::Permissions::from_mode(mode);
        if let Err(e) = writer.set_permissions(perms) {
            eprintln!(
                "[WARN] Failed to restore permissions on {:?}: {e}",
                dest.path.join(dst_name),
            );
        }
    }

    // Restore timestamps via the file handle (avoids path-based TOCTOU).
    restore_timestamps(&writer, metadata, &dest.path.join(dst_name));

    // Fsync before unlinking the source so the new file is durable.
    writer.sync_all()?;

    Ok(())
}

/// Look up the filesystem type via `fstatfs` on Linux/Android. Other Unix
/// platforms don't expose a portable `f_type` magic, so we report `"unknown"`.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn detect_fs_type(dir_fd: &OwnedFd) -> String {
    match rustix::fs::fstatfs(dir_fd) {
        Ok(statfs) => {
            #[allow(clippy::cast_sign_loss)]
            let magic = statfs.f_type as u64;
            fs_magic_to_name(magic)
                .map_or_else(|| format!("unknown (0x{magic:x})"), ToOwned::to_owned)
        },
        Err(_) => "unknown".to_string(),
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn detect_fs_type(_dir_fd: &OwnedFd) -> String {
    "unknown".to_string()
}

/// Map a Linux `fstatfs` `f_type` magic number to a human-readable name.
/// Values from `<linux/magic.h>`.
#[cfg(any(target_os = "linux", target_os = "android"))]
const fn fs_magic_to_name(magic: u64) -> Option<&'static str> {
    Some(match magic {
        0xEF53 => "ext2/3/4",
        0x9123_683E => "btrfs",
        0x5846_5342 => "xfs",
        0xF2F5_2010 => "f2fs",
        0x0102_1994 => "tmpfs",
        0x4D44 => "vfat/msdos",
        0x5346_544E => "ntfs",
        0x7366_746E => "ntfs3",
        0x6969 => "nfs",
        0xFF53_4D42 => "cifs/smb",
        0xFE53_4D42 => "smb2",
        0x517B => "smb",
        0x6573_5546 => "fuse",
        0x7371_7368 => "squashfs",
        0x794C_7630 => "overlayfs",
        0xCA45_1A4E => "bcachefs",
        0x2FC1_2FC1 => "zfs",
        0x5346_414F => "afs",
        0xE0F5_E1E2 => "erofs",
        0x0102_1997 => "9p",
        0x3434 => "nilfs",
        0x5265_4973 => "reiserfs",
        0x8584_58F6 => "ramfs",
        0xF15F => "ecryptfs",
        0x9660 => "isofs",
        0x72B6 => "jffs2",
        0x28CD_3D45 => "cramfs",
        0x9FA0 => "proc",
        0x6265_6572 => "sysfs",
        0x1373 => "devtmpfs",
        0x1CD1 => "devpts",
        0x6367_7270 => "cgroup2",
        0x0027_E0EB => "cgroup",
        0x7363_6673 => "securityfs",
        0x6462_6720 => "debugfs",
        0x7472_6163 => "tracefs",
        0x4244 => "hfs",
        0x6E73_6673 => "nsfs",
        0xCAFE_4A11 => "bpf",
        _ => return None,
    })
}

/// Helper to detect if an error is "Filename too long".
fn is_name_too_long(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::InvalidFilename
        || err.raw_os_error() == Some(libc::ENAMETOOLONG)
}

/// Truncate a filename to fit within `MAX_FILENAME_BYTES`, preserving extension if possible.
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
    format!("{truncated_base}{ext}")
}

/// Truncate a string to fit within a maximum byte limit, respecting UTF-8 boundaries.
const fn truncate_str_to_byte_limit(s: &str, max_bytes: usize) -> &str {
    s.split_at(s.floor_char_boundary(max_bytes)).0
}

#[must_use]
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
                (u128::from(device_id) << 64) | u128::from(inode_number)
            },
            FileId::LowRes { volume_serial_number, file_index } => {
                (u128::from(volume_serial_number) << 64) | u128::from(file_index)
            },
            FileId::HighRes { volume_serial_number, file_id } => {
                file_id ^ (u128::from(volume_serial_number) << 64)
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_str_respects_utf8_boundaries() {
        let s = "aé😊b"; // 'a' (1), 'é' (2), '😊' (4), 'b' (1) = 8 bytes
        assert_eq!(truncate_str_to_byte_limit(s, 0), "");
        assert_eq!(truncate_str_to_byte_limit(s, 1), "a");
        assert_eq!(truncate_str_to_byte_limit(s, 2), "a");
        assert_eq!(truncate_str_to_byte_limit(s, 3), "aé");
        assert_eq!(truncate_str_to_byte_limit(s, 6), "aé");
        assert_eq!(truncate_str_to_byte_limit(s, 7), "aé😊");
        assert_eq!(truncate_str_to_byte_limit(s, 8), "aé😊b");
        assert_eq!(truncate_str_to_byte_limit(s, 100), "aé😊b");
    }

    #[test]
    fn truncate_filename_preserves_extension_and_fits_limit() {
        let long_stem = "a".repeat(300);
        let name = format!("{long_stem}.jpeg");
        let truncated = truncate_filename_to_limit(&name);
        assert_eq!(truncated.len(), MAX_FILENAME_BYTES);
        assert!(truncated.ends_with(".jpeg"));
    }

    #[test]
    fn move_files_into_moves_and_refuses_overwrite() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();

        let src_file = src_dir.path().join("photo.jpg");
        std::fs::write(&src_file, b"image-bytes").unwrap();

        let dest = DestinationDir::open(dst_dir.path()).unwrap();
        let results = move_files_into(&dest, std::slice::from_ref(&src_file));
        assert_eq!(results.len(), 1);
        assert!(results[0].outcome.is_ok());
        assert!(!src_file.exists());
        assert_eq!(std::fs::read(dst_dir.path().join("photo.jpg")).unwrap(), b"image-bytes");

        // Second file with same name must fail with AlreadyExists and leave both intact
        let src_file2 = src_dir.path().join("photo.jpg");
        std::fs::write(&src_file2, b"new-bytes").unwrap();
        let results2 = move_files_into(&dest, std::slice::from_ref(&src_file2));
        assert!(results2[0].outcome.is_err());
        assert_eq!(
            results2[0].outcome.as_ref().unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(&src_file2).unwrap(), b"new-bytes");
        assert_eq!(std::fs::read(dst_dir.path().join("photo.jpg")).unwrap(), b"image-bytes");
    }
}
