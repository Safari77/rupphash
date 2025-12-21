phdupes
=======

`phdupes`: Rust duplicate image detector using standard phash (perceptual image
hash) and pdqhash implementations, includes phdupes app: rust GUI (egui) and
TUI (ratatui)  for viewing and interacting with found duplicates, multithreaded
scanning and (also RAW) image processing, configurable preload of images,
metadata caching (encrypted with XChaCha20Poly1305, LMDB database),
Search inside Exif tags (also supports derived values like Country (from GPS location),
and Sun Azimuth, Sun Altitude (from GPS date, location, and the location’s timezone).

A million files takes about 2.5 GiB of memory.
500,000 files takes about 15-20s to Group on a modern CPU (after all the data has been loaded).
For hard-linked files PDQ features is stored only once (per file data) in memory.

Duplicates can be found by content (perceptual), or idendical pixel data
(--pixel-hash converts data to 16bit values for comparison),
or whole file comparison (bit-identical, using blake3).

## GPS Map
Examples:
```
[map_providers]
tileserver = "http://127.0.0.1:17766/styles/basic-preview/{z}/{x}/{y}@2x.png"
maptiler = "https://api.maptiler.com/maps/topo-v4/{z}/{x}/{y}@2x.png?key=GETYOUROWN"
```

If you have `/mydata/tiles/finland.mbtiles` generated with
`java -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=3128 -Xmx4g -jar planetiler.jar --download --area=finland --output=finland.mbtiles`:

```bash
podman run --rm -it -p 17766:17766 -v "/mydata/tiles:/data:z" maptiler/tileserver-gl --verbose -b 0.0.0.0 -p 17766 --mbtiles /data/finland.mbtiles
```
Open http://127.0.0.1:17766/ to view supported styles.

## The fonts
To get the latest
```bash
curl -s 'https://api.github.com/repos/be5invis/Sarasa-Gothic/releases/latest' | jq -r ".assets[] | .browser_download_url" | pcre2grep "/Sarasa-TTC-Unhinted-.*7z$" | xargs -n 1 curl -L -O --fail --silent --show-error --
7z e Sarasa-TTC-Unhinted-*.7z
fc-query --format='%{index}: %{family} %{style}\n' Sarasa-Regular.ttc | pcre2grep "Sarasa (Term|UI) SC"
7: Sarasa UI SC,更纱黑体 UI SC Regular
25: Sarasa Term SC Regular
```

Configuration:
[gui]
font_monospace = "/usr/share/fonts/misc/Sarasa/Sarasa-Regular.ttc"
font_ui = "/usr/share/fonts/misc/Sarasa/Sarasa-Regular.ttc"

You can also embed it into binary so font configuration is not needed:
`cargo build --release --features embed-fonts`

## LFS
Using git-lfs now.  For a smaller clone without LFS files:
`GIT_LFS_SKIP_SMUDGE=1 git clone https://github.com/Safari77/rupphash.git`
Fetch what you need:
`git lfs pull --include="assets/fonts"`
(This is a placeholder notice, I am not currently using LFS.)

## Windows
If you want to use the GNU Toolchain (MinGW):

Install MSYS2: https://www.msys2.org/
Open "MSYS2 UCRT64" terminal (should be now in Start Menu)
*Note*: SHIFT-INSERT is paste operation by default
Run `pacman -S mingw-w64-x86_64-toolchain` (needs about 900 MiB)
`ls /c/msys64/mingw64/bin/dlltool.exe` show show the execuble.
Add `C:\msys64\mingw64\bin` into system Path environment.

Then back to rupphash directory!
```bash
rustup target add x86_64-pc-windows-gnu
cargo build --target x86_64-pc-windows-gnu
```

You might need to disable HEIF and/or rsraw manually, or you could use MSVC target.

*Note*:
MSVC Target: uses the linker built into Visual Studio (link.exe).

GNU Target: Uses the GNU linker (ld.exe) and tools (dlltool.exe).
Rust does not bundle these; it expects you to provide them.

## Screenshot - View mode
![Screenshot view](phdupes-view.webp)

## Screenshot - Duplicate finding mode
![Screenshot dupe1](phdupes-dupe1.webp)
![Screenshot dupe2](phdupes-dupe2.webp)
