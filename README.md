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

## Screenshot - View mode
![Screenshot view](phdupes-view.webp)

## Screenshot - Duplicate finding mode
![Screenshot dupe1](phdupes-dupe1.webp)
![Screenshot dupe2](phdupes-dupe2.webp)
