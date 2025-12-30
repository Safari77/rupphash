mod exif_extract;
mod exif_types;
mod helper_exif;
mod image_features;
mod position;

use clap::Parser;
use exif::Reader;
use std::fs::File;
use std::io::BufReader;

/// Simple program to calculate sun position from EXIF data
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the image file
    #[arg(required = true)]
    filename: String,

    /// Use GPS timestamp (UTC) instead of DateTimeOriginal
    #[arg(long)]
    gpstime: bool,
}

fn main() {
    let loc_helsinki = (60.1699, 24.9384);
    let loc_tampere = (61.4978, 23.7610);
    let (dist, bearing) = position::distance_and_bearing(loc_helsinki, loc_tampere);
    println!(
        "Helsinki to Tampere Distance: {:.3} km, Compass Bearing: {:.3}°",
        dist / 1000.0,
        bearing
    );

    // 1. Parse arguments using Clap
    let args = Args::parse();
    let filename = &args.filename;

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

    let coords = exif_extract::extract_gps_lat_lon(&exif);
    let alt = helper_exif::get_altitude(&exif);

    // 2. Pass the gpstime flag to our date parser
    let date_str = helper_exif::get_date_str(&exif, args.gpstime);

    match (coords, date_str.clone()) {
        (Some((latitude, longitude)), Some(clean_date)) => {
            let altitude_val = alt.unwrap_or(0.0);

            println!("--- Input Data ---");
            println!("File:       {}", filename);
            println!("Lat/Lon:    {:.5}, {:.5}", latitude, longitude);
            println!("Altitude:   {:.1} m", altitude_val);
            println!(
                "Date Clean: '{}' {}",
                clean_date,
                if args.gpstime { "(GPS UTC)" } else { "" }
            );

            match position::sun_alt_and_azimuth(&clean_date, latitude, longitude, alt, args.gpstime)
            {
                Ok((sun_alt, sun_az, tzstring)) => {
                    println!("\n--- Result ---");
                    println!("Sun Altitude:  {:.4} deg", sun_alt);
                    println!("Sun Azimuth:   {:.4} deg", sun_az);
                    println!("Timezone:      {}", tzstring);
                }
                Err(e) => {
                    println!("\n--- Calculation Error ---");
                    println!("Error: {}", e);
                }
            }
        }
        _ => {
            println!("--- Missing Data ---");
            if coords.is_none() {
                println!("Error: GPS coordinates missing");
            }
            if date_str.is_none() {
                println!(
                    "Error: Date missing (Mode: {})",
                    if args.gpstime { "GPS" } else { "Original" }
                );
            }
        }
    }
}
