mod position;

use std::env;
use std::fs::File;
use std::io::BufReader;
use exif::{In, Reader, Tag, Value};

fn main() {
    let loc_helsinki = (60.1699, 24.9384);
    let loc_tampere = (61.4978, 23.7610);
    let (dist, bearing) = position::distance_and_bearing(loc_helsinki, loc_tampere);
    println!("Helsinki to Tampere Distance: {:.3} km, Compass Bearing: {:.3}°", dist / 1000.0, bearing);

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("To also test Sun position/Azimuth, give path to image with EXIF data as argmuent");
        std::process::exit(1);
    }
    let filename = &args[1];

    let file = File::open(filename).expect("Cannot open file");
    let mut bufreader = BufReader::new(file);
    let exif_reader = Reader::new();

    let exif = match exif_reader.read_from_container(&mut bufreader) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Error reading EXIF data: {}", e);
            std::process::exit(1);
        }
    };

    let lat = get_gps_coord(&exif, Tag::GPSLatitude, Tag::GPSLatitudeRef);
    let lon = get_gps_coord(&exif, Tag::GPSLongitude, Tag::GPSLongitudeRef);
    let alt = get_altitude(&exif);
    let date_str = get_date_str(&exif); 

    match (lat, lon, date_str.clone()) {
        (Some(latitude), Some(longitude), Some(clean_date)) => {
            let altitude_val = alt.unwrap_or(0.0);

            println!("--- Input Data ---");
            println!("File:       {}", filename);
            println!("Lat/Lon:    {:.5}, {:.5}", latitude, longitude);
            println!("Altitude:   {:.1} m", altitude_val);
            println!("Date Clean: '{}'", clean_date);

            // Pass the parsed altitude (or None, which defaults to 0.0 inside the function)
            match position::sun_alt_and_azimuth(&clean_date, latitude, longitude, alt) {
                Ok((sun_alt, sun_az, tzstring)) => {
                    println!("\n--- Result ---");
                    println!("Sun Altitude:  {:.4} deg", sun_alt);
                    println!("Sun Azimuth:   {:.4} deg", sun_az);
                    println!("Timezone:      {}", tzstring);
                },
                Err(e) => {
                    println!("\n--- Calculation Error ---");
                    println!("Error: {}", e);
                }
            }
        },
        _ => {
            println!("--- Missing Data ---");
            if lat.is_none() { println!("Error: Latitude missing"); }
            if lon.is_none() { println!("Error: Longitude missing"); }
            if date_str.is_none() { println!("Error: Date missing"); }
        }
    }
}

/// Uses `exif::DateTime` to parse standard EXIF dates.
/// Returns a standard ISO-like string "YYYY-MM-DD HH:MM:SS" which Jiff/Chrono likes.
fn get_date_str(exif: &exif::Exif) -> Option<String> {
    let field = exif.get_field(Tag::DateTimeOriginal, In::PRIMARY)?;

    match field.value {
        Value::Ascii(ref vec) if !vec.is_empty() => {
            // Use the crate's built-in parser
            if let Ok(dt) = exif::DateTime::from_ascii(&vec[0]) {
                // Format it cleanly for our position module
                return Some(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
                ));
            }
        }
        _ => {}
    }
    None
}

/// Parses GPS tags safely handling division by zero and sign references.
fn get_gps_coord(exif: &exif::Exif, val_tag: Tag, ref_tag: Tag) -> Option<f64> {
    let val_field = exif.get_field(val_tag, In::PRIMARY)?;

    // 1. Parse numeric value (Degrees, Minutes, Seconds)
    let mut decimal = if let Value::Rational(ref rats) = val_field.value {
        if rats.len() >= 3 {
            if rats[0].denom == 0 || rats[1].denom == 0 || rats[2].denom == 0 {
                return None;
            }

            let degrees = rats[0].num as f64 / rats[0].denom as f64;
            let minutes = rats[1].num as f64 / rats[1].denom as f64;
            let seconds = rats[2].num as f64 / rats[2].denom as f64;

            degrees + (minutes / 60.0) + (seconds / 3600.0)
        } else {
            return None;
        }
    } else {
        return None;
    };

    // 2. Apply Reference Sign (S or W = negative)
    if let Some(ref_field) = exif.get_field(ref_tag, In::PRIMARY) {
        let ref_str = ref_field.display_value().to_string();
        if ref_str.contains('S') || ref_str.contains('W') {
            decimal = -decimal;
        }
    }

    Some(decimal)
}

fn get_altitude(exif: &exif::Exif) -> Option<f64> {
    let val_field = exif.get_field(Tag::GPSAltitude, In::PRIMARY)?;
    let ref_field = exif.get_field(Tag::GPSAltitudeRef, In::PRIMARY);

    if let Value::Rational(ref rats) = val_field.value {
        if rats.is_empty() || rats[0].denom == 0 { return None; }

        let mut alt = rats[0].num as f64 / rats[0].denom as f64;

        if let Some(rf) = ref_field {
            if let Value::Byte(ref bytes) = rf.value {
                if !bytes.is_empty() && bytes[0] == 1 {
                    alt = -alt;
                }
            }
        }
        return Some(alt);
    }
    None
}
