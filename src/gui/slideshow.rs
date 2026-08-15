use clap::ValueEnum;
use eframe::egui;
use egui_wgpu::wgpu;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration choice for slideshow transitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum SlideshowEffectChoice {
    #[default]
    Random,
    None,
    KenBurns,
    SwirlOut,
    SwirlIn,
    Shards,
    PixelBoom,
    Explode,
}

impl SlideshowEffectChoice {
    pub const VALID_CHOICES: [&'static str; 8] =
        ["none", "kenburns", "swirlin", "swirlout", "shards", "pixelboom", "explode", "random"];

    pub fn try_from_str(s: &str) -> Result<Self, String> {
        let clean = s.trim().to_lowercase().replace(['-', '_', ' '], "");
        match clean.as_str() {
            "none" | "off" | "cut" => Ok(SlideshowEffectChoice::None),
            "kenburns" | "kenburn" | "panzoom" => Ok(SlideshowEffectChoice::KenBurns),
            "swirl" | "swirlout" | "vortex" | "vortexout" => Ok(SlideshowEffectChoice::SwirlOut),
            "swirlin" | "vortexin" => Ok(SlideshowEffectChoice::SwirlIn),
            "shards" | "shard" | "glass" | "shatter" => Ok(SlideshowEffectChoice::Shards),
            "pixelboom" | "pixelblast" | "boom" | "pixels" => Ok(SlideshowEffectChoice::PixelBoom),
            "explode" | "explosion" | "blast" => Ok(SlideshowEffectChoice::Explode),
            "random" | "all" | "any" => Ok(SlideshowEffectChoice::Random),
            _ => Err(format!(
                "Invalid slideshow effect '{}'. Use one of: {}",
                s,
                Self::VALID_CHOICES.join(", ")
            )),
        }
    }

    pub fn from_str_loose(s: &str) -> Self {
        Self::try_from_str(s).unwrap_or(SlideshowEffectChoice::Random)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SlideshowEffectChoice::None => "none",
            SlideshowEffectChoice::KenBurns => "ken-burns",
            SlideshowEffectChoice::SwirlOut => "swirl-out",
            SlideshowEffectChoice::SwirlIn => "swirl-in",
            SlideshowEffectChoice::Shards => "shards",
            SlideshowEffectChoice::PixelBoom => "pixel-boom",
            SlideshowEffectChoice::Explode => "explode",
            SlideshowEffectChoice::Random => "random",
        }
    }

    pub fn pick_effective(&self, last_effect: Option<SlideshowEffect>) -> SlideshowEffect {
        match self {
            SlideshowEffectChoice::None => SlideshowEffect::None,
            SlideshowEffectChoice::KenBurns => SlideshowEffect::KenBurns,
            SlideshowEffectChoice::SwirlOut => SlideshowEffect::SwirlOut,
            SlideshowEffectChoice::SwirlIn => SlideshowEffect::SwirlIn,
            SlideshowEffectChoice::Shards => SlideshowEffect::Shards,
            SlideshowEffectChoice::PixelBoom => SlideshowEffect::PixelBoom,
            SlideshowEffectChoice::Explode => SlideshowEffect::Explode,
            SlideshowEffectChoice::Random => {
                let available = SlideshowEffect::ALL;
                let idx = (random_u64() as usize) % available.len();
                let chosen = available[idx];
                if Some(chosen) == last_effect && available.len() > 1 {
                    available[(idx + 1) % available.len()]
                } else {
                    chosen
                }
            }
        }
    }
}

impl FromStr for SlideshowEffectChoice {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from_str(s)
    }
}

/// Individual transition effect types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SlideshowEffect {
    #[default]
    None,
    KenBurns,
    SwirlOut,
    SwirlIn,
    Shards,
    PixelBoom,
    Explode,
}

impl SlideshowEffect {
    pub const ALL: [SlideshowEffect; 6] = [
        SlideshowEffect::KenBurns,
        SlideshowEffect::SwirlOut,
        SlideshowEffect::SwirlIn,
        SlideshowEffect::Shards,
        SlideshowEffect::PixelBoom,
        SlideshowEffect::Explode,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            SlideshowEffect::None => "None",
            SlideshowEffect::KenBurns => "Ken Burns",
            SlideshowEffect::SwirlOut => "Swirl Out",
            SlideshowEffect::SwirlIn => "Swirl In",
            SlideshowEffect::Shards => "Shards",
            SlideshowEffect::PixelBoom => "Pixel Boom",
            SlideshowEffect::Explode => "Explode",
        }
    }

    pub fn effect_type_id(&self) -> u32 {
        match self {
            SlideshowEffect::None => 0,
            SlideshowEffect::KenBurns => 1,
            SlideshowEffect::SwirlOut => 2,
            SlideshowEffect::SwirlIn => 3,
            SlideshowEffect::Shards => 4,
            SlideshowEffect::PixelBoom => 5,
            SlideshowEffect::Explode => 6,
        }
    }
}

/// Thread-safe pseudo-random number generator (SplitMix64)
fn random_u64() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0x853c49e6748fea9b);
    let mut x = SEED.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    x
}

fn random_f32(min: f32, max: f32) -> f32 {
    let t = (random_u64() as f64 / u64::MAX as f64) as f32;
    min + (max - min) * t
}

/// GPU Uniforms matching WGSL shader memory layout (48 bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SlideshowUniforms {
    pub progress: f32,
    pub view_aspect: f32,
    pub curr_aspect: f32,
    pub next_aspect: f32,
    pub effect_type: u32,
    pub direction: f32,
    pub param1: f32, // KenBurns: zoom_start
    pub param2: f32, // KenBurns: zoom_end
    pub param3: f32, // KenBurns: pan_start_x
    pub param4: f32, // KenBurns: pan_start_y
    pub param5: f32, // KenBurns: pan_end_x
    pub param6: f32, // KenBurns: pan_end_y
}

impl Default for SlideshowUniforms {
    fn default() -> Self {
        Self {
            progress: 0.0,
            view_aspect: 1.0,
            curr_aspect: 1.0,
            next_aspect: 1.0,
            effect_type: 0,
            direction: 1.0,
            param1: 1.0,
            param2: 1.2,
            param3: 0.0,
            param4: 0.0,
            param5: 0.05,
            param6: 0.05,
        }
    }
}

const SLIDESHOW_SHADER_WGSL: &str = r#"
struct Uniforms {
    progress: f32,
    view_aspect: f32,   // viewport width / height
    curr_aspect: f32,   // current image width / height
    next_aspect: f32,   // next image width / height
    effect_type: u32,   // 0 = None/Crossfade, 1 = Ken Burns, 2 = Swirl Out, 3 = Swirl In, 4 = Shards, 5 = Pixel Boom, 6 = Explode
    direction: f32,     // 1.0 or -1.0
    param1: f32,        // KenBurns: zoom_start
    param2: f32,        // KenBurns: zoom_end
    param3: f32,        // KenBurns: pan_start_x
    param4: f32,        // KenBurns: pan_start_y
    param5: f32,        // KenBurns: pan_end_x
    param6: f32,        // KenBurns: pan_end_y
};

@group(0) @binding(0) var<uniform> u: Uniforms;
@group(0) @binding(1) var s_sampler: sampler;
@group(0) @binding(2) var t_current: texture_2d<f32>;
@group(0) @binding(3) var t_next: texture_2d<f32>;

struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) uv: vec2<f32>,
};

@vertex
fn vs_main(@builtin(vertex_index) vertex_index: u32) -> VertexOutput {
    var pos = array<vec2<f32>, 6>(
        vec2<f32>(-1.0, -1.0),
        vec2<f32>( 1.0, -1.0),
        vec2<f32>(-1.0,  1.0),
        vec2<f32>(-1.0,  1.0),
        vec2<f32>( 1.0, -1.0),
        vec2<f32>( 1.0,  1.0),
    );
    var uvs = array<vec2<f32>, 6>(
        vec2<f32>(0.0, 1.0),
        vec2<f32>(1.0, 1.0),
        vec2<f32>(0.0, 0.0),
        vec2<f32>(0.0, 0.0),
        vec2<f32>(1.0, 1.0),
        vec2<f32>(1.0, 0.0),
    );
    var out: VertexOutput;
    out.position = vec4<f32>(pos[vertex_index], 0.0, 1.0);
    out.uv = uvs[vertex_index];
    return out;
}

fn fit_uv(uv: vec2<f32>, img_aspect: f32, view_aspect: f32) -> vec2<f32> {
    var res = uv;
    if (view_aspect > img_aspect) {
        // Viewport is wider than image: pillarbox (bars on left & right)
        let scale = view_aspect / max(img_aspect, 0.001);
        res.x = (res.x - 0.5) * scale + 0.5;
    } else {
        // Viewport is taller than image: letterbox (bars on top & bottom)
        let scale = img_aspect / max(view_aspect, 0.001);
        res.y = (res.y - 0.5) * scale + 0.5;
    }
    return res;
}

fn sample_image(tex: texture_2d<f32>, smp: sampler, uv: vec2<f32>) -> vec4<f32> {
    if (uv.x < 0.0 || uv.x > 1.0 || uv.y < 0.0 || uv.y > 1.0) {
        return vec4<f32>(0.0, 0.0, 0.0, 1.0);
    }
    return textureSample(tex, smp, uv);
}

fn hash22(p: vec2<f32>) -> vec2<f32> {
    var p3 = fract(vec3<f32>(p.xyx) * vec3<f32>(443.897, 441.423, 437.195));
    p3 = p3 + dot(p3, p3.yzx + 19.19);
    return fract((p3.xx + p3.yz) * p3.zy);
}

@fragment
fn fs_main(in: VertexOutput) -> @location(0) vec4<f32> {
    let uv = in.uv;
    let t = clamp(u.progress, 0.0, 1.0);

    if (u.effect_type == 1u) {
        // --- KEN BURNS EFFECT (Pan & Zoom transition) ---
        let zoom = mix(u.param1, u.param2, t);
        let pan_x = mix(u.param3, u.param5, t);
        let pan_y = mix(u.param4, u.param6, t);

        let kb_viewport_uv = (uv - vec2<f32>(0.5, 0.5)) / max(zoom, 0.001) + vec2<f32>(0.5, 0.5) + vec2<f32>(pan_x, pan_y);
        let curr_uv = fit_uv(kb_viewport_uv, u.curr_aspect, u.view_aspect);
        let col_curr = sample_image(t_current, s_sampler, curr_uv);

        let fade = smoothstep(0.15, 0.85, t);
        let next_zoom = mix(u.param2 * 0.95, 1.0, t);
        let next_viewport_uv = (uv - vec2<f32>(0.5, 0.5)) / max(next_zoom, 0.001) + vec2<f32>(0.5, 0.5);
        let next_uv = fit_uv(next_viewport_uv, u.next_aspect, u.view_aspect);
        let col_next = sample_image(t_next, s_sampler, next_uv);

        return mix(col_curr, col_next, fade);
    } else if (u.effect_type == 2u) {
        // --- SWIRL OUT EFFECT (Vortex Twist & Pinch) ---
        let center = vec2<f32>(0.5, 0.5);
        var offset = uv - center;
        offset.x = offset.x * u.view_aspect;

        let r = length(offset);
        let theta = atan2(offset.y, offset.x);

        let max_radius = 0.85;
        let norm_r = clamp(1.0 - r / max_radius, 0.0, 1.0);
        let swirl_angle = norm_r * norm_r * t * 14.0 * u.direction;
        let pinch = 1.0 + t * t * 2.5;
        let distorted_r = r * pinch;

        let distorted_offset = vec2<f32>(
            cos(theta + swirl_angle) * distorted_r / u.view_aspect,
            sin(theta + swirl_angle) * distorted_r
        );

        let uv_out = fit_uv(center + distorted_offset, u.curr_aspect, u.view_aspect);
        let col_out = sample_image(t_current, s_sampler, uv_out);

        // Incoming image unraveling from counter-twist
        let in_t = 1.0 - t;
        let in_swirl = norm_r * in_t * in_t * -4.0 * u.direction;
        let in_offset = vec2<f32>(
            cos(theta + in_swirl) * r / u.view_aspect,
            sin(theta + in_swirl) * r
        );
        let uv_in = fit_uv(center + in_offset, u.next_aspect, u.view_aspect);
        let col_in = sample_image(t_next, s_sampler, uv_in);

        let fade = smoothstep(0.15, 0.85, t);
        return mix(col_out, col_in, fade);
    } else if (u.effect_type == 3u) {
        // --- SWIRL IN EFFECT (Inward Vortex Drain & Implosion) ---
        let center = vec2<f32>(0.5, 0.5);
        var offset = uv - center;
        offset.x = offset.x * u.view_aspect;

        let r = length(offset);
        let theta = atan2(offset.y, offset.x);

        let max_radius = 0.95;
        let norm_r = clamp(1.0 - r / max_radius, 0.0, 1.0);
        let swirl_angle = -norm_r * norm_r * (1.0 + norm_r) * t * 16.0 * u.direction;
        let suck = max(1.0 - t * t * 0.85, 0.02);
        let distorted_r = r / suck;

        let distorted_offset = vec2<f32>(
            cos(theta + swirl_angle) * distorted_r / u.view_aspect,
            sin(theta + swirl_angle) * distorted_r
        );

        let uv_out = fit_uv(center + distorted_offset, u.curr_aspect, u.view_aspect);
        let col_out = sample_image(t_current, s_sampler, uv_out);

        // Incoming image spirals outward from vortex center
        let in_t = 1.0 - t;
        let in_swirl = norm_r * in_t * in_t * 10.0 * u.direction;
        let in_scale = 1.0 + in_t * in_t * 1.5;
        let in_r = r * in_scale;
        let in_offset = vec2<f32>(
            cos(theta + in_swirl) * in_r / u.view_aspect,
            sin(theta + in_swirl) * in_r
        );
        let uv_in = fit_uv(center + in_offset, u.next_aspect, u.view_aspect);
        let col_in = sample_image(t_next, s_sampler, uv_in);

        let fade = smoothstep(0.15, 0.85, t);
        return mix(col_out, col_in, fade);
    } else if (u.effect_type == 4u) {
        // --- SHARDS EFFECT (Glass Exploding into Shards) ---
        let cols = 6.0;
        let rows = 5.0;
        var hit_shard = false;
        var hit_uv = uv;
        var hit_edge = 0.0;
        var best_z = -999.0;
        var hit_alpha = 1.0;

        let explosion = pow(t, 1.35);

        for (var gy = 0; gy < 5; gy = gy + 1) {
            for (var gx = 0; gx < 6; gx = gx + 1) {
                let cell_coord = vec2<f32>(f32(gx), f32(gy));
                let rnd = hash22(cell_coord + vec2<f32>(1.7, 9.2));
                let shard_center = (cell_coord + vec2<f32>(0.5, 0.5) + (rnd - 0.5) * 0.4) / vec2<f32>(cols, rows);

                // Shard trajectory radiating from viewport center
                let from_center = shard_center - vec2<f32>(0.5, 0.5);
                let dist_c = length(vec2<f32>(from_center.x * u.view_aspect, from_center.y));
                var dir = select(normalize(from_center), vec2<f32>(0.0, 1.0), dist_c < 0.001);
                dir = normalize(dir + (rnd - 0.5) * 0.7);

                let speed = 0.35 + rnd.x * 0.65;
                let offset = (dir * (0.25 + dist_c * 0.75) * speed) * explosion;
                let gravity = vec2<f32>(0.0, explosion * explosion * 0.35);
                let total_offset = offset + gravity;

                let rot = (rnd.y - 0.5) * 6.0 * explosion * u.direction;
                let scale = max(1.0 - explosion * (0.15 + rnd.x * 0.25), 0.01);

                // Transform fragment coordinates back to resting frame
                let delta = uv - (shard_center + total_offset);
                let delta_asp = vec2<f32>(delta.x * u.view_aspect, delta.y) / scale;
                let cos_r = cos(-rot);
                let sin_r = sin(-rot);
                let rot_asp = vec2<f32>(
                    delta_asp.x * cos_r - delta_asp.y * sin_r,
                    delta_asp.x * sin_r + delta_asp.y * cos_r
                );
                let p_rest = shard_center + vec2<f32>(rot_asp.x / u.view_aspect, rot_asp.y);

                let rest_grid = p_rest * vec2<f32>(cols, rows);
                let cell_idx = floor(rest_grid);
                if (i32(cell_idx.x) == gx && i32(cell_idx.y) == gy) {
                    let cell_fract = fract(rest_grid);
                    let facet = sin(p_rest.x * 25.0 + rnd.x * 6.28) * cos(p_rest.y * 25.0 + rnd.y * 6.28) * 0.05;
                    let base_edge = min(min(cell_fract.x, 1.0 - cell_fract.x), min(cell_fract.y, 1.0 - cell_fract.y));
                    let edge_dist = base_edge + facet * smoothstep(0.0, 0.2, explosion);
                    let margin = explosion * 0.07;

                    if (edge_dist > margin || explosion < 0.001) {
                        let z = rnd.x * 0.7 + rnd.y * 0.3;
                        if (z > best_z) {
                            best_z = z;
                            hit_shard = true;
                            hit_uv = p_rest;
                            hit_edge = smoothstep(0.08, 0.0, edge_dist - margin) * smoothstep(0.0, 0.15, explosion);
                            hit_alpha = clamp(1.0 - explosion * 1.15 + rnd.y * 0.2, 0.0, 1.0);
                        }
                    }
                }
            }
        }

        let uv_next = fit_uv(uv, u.next_aspect, u.view_aspect);
        let col_next = sample_image(t_next, s_sampler, uv_next);

        if (hit_shard) {
            let uv_curr = fit_uv(hit_uv, u.curr_aspect, u.view_aspect);
            var col_curr = sample_image(t_current, s_sampler, uv_curr);
            // Glass shard specular edge shimmer
            col_curr = col_curr + vec4<f32>(vec3<f32>(hit_edge * 0.6), 0.0);
            return mix(col_next, col_curr, hit_alpha);
        } else {
            return col_next;
        }
    } else if (u.effect_type == 5u) {
        // --- PIXEL BOOM EFFECT (Pixel Disintegration & Radial Particle Blast) ---
        let grid = vec2<f32>(42.0 * u.view_aspect, 42.0);
        let cell = floor(uv * grid);
        let cell_center = (cell + vec2<f32>(0.5, 0.5)) / grid;
        let rnd = hash22(cell + vec2<f32>(3.1415, 7.8923));

        var from_center = cell_center - vec2<f32>(0.5, 0.5);
        let dist = length(vec2<f32>(from_center.x * u.view_aspect, from_center.y));
        var dir = select(normalize(from_center), vec2<f32>(0.0, 1.0), dist < 0.001);
        dir = normalize(dir + (rnd - 0.5) * 0.85);

        // Blast wavefront propagates outward from center
        let blast_speed = 1.3;
        let trigger_time = dist / blast_speed;
        let local_t = clamp((t - trigger_time * 0.55) / 0.5, 0.0, 1.0);
        let blast_power = pow(local_t, 1.7);

        let disp = (dir * (0.35 + dist * 0.9) + (rnd - 0.5) * 0.4) * blast_power * (0.8 + rnd.x * 0.65);
        let particle_uv = uv - disp;

        let uv_next = fit_uv(uv, u.next_aspect, u.view_aspect);
        let col_next = sample_image(t_next, s_sampler, uv_next);

        let uv_curr = fit_uv(particle_uv, u.curr_aspect, u.view_aspect);
        var col_curr = sample_image(t_current, s_sampler, uv_curr);

        // Particle fade out as they disperse
        let particle_alpha = clamp(1.0 - blast_power * 1.3 + rnd.y * 0.15, 0.0, 1.0);

        // Radial shockwave ring & central flash
        let wave_dist = t * blast_speed;
        let shockwave = smoothstep(0.12, 0.0, abs(dist - wave_dist)) * (1.0 - t) * 0.45;
        let flash = smoothstep(0.22, 0.0, t) * max(1.0 - dist * 2.0, 0.0) * 0.35;

        let col_blast = col_curr + vec4<f32>(vec3<f32>(shockwave + flash), 0.0);
        return mix(col_next, col_blast, particle_alpha);
    } else if (u.effect_type == 6u) {
        // --- EXPLODE EFFECT (Continuous Radial Blast Wave & Outward Pixel Ejection) ---
        let center = vec2<f32>(0.5, 0.5);
        var offset = uv - center;
        offset.x = offset.x * u.view_aspect;

        let r = length(offset);
        let theta = atan2(offset.y, offset.x);

        // Blast propagation wavefront
        let blast_t = pow(t, 1.3);
        let blast_radius = blast_t * 1.75;

        // Angular turbulence for ragged blast wave
        let angle_noise = sin(theta * 8.0 + u.direction * 3.0) * 0.08 
                        + cos(theta * 17.0 - u.direction * 5.0) * 0.04;
        let effective_radius = r + angle_noise * smoothstep(0.0, 0.3, t);

        // Displace pixels outward: fragment at r samples from a smaller radius (inward)
        let push_power = blast_t * (2.8 + (1.0 - clamp(r, 0.0, 1.0)) * 1.5);
        let distorted_r = r / max(1.0 + push_power * 3.2, 0.001);

        let blast_offset = vec2<f32>(
            cos(theta) * distorted_r / u.view_aspect,
            sin(theta) * distorted_r
        );

        let uv_curr_blast = fit_uv(center + blast_offset, u.curr_aspect, u.view_aspect);
        var col_curr = sample_image(t_current, s_sampler, uv_curr_blast);

        // Chromatic aberration along radial blast lines
        let ca_offset = 0.015 * blast_t;
        let uv_ca_r = fit_uv(center + blast_offset * (1.0 + ca_offset), u.curr_aspect, u.view_aspect);
        let uv_ca_b = fit_uv(center + blast_offset * (1.0 - ca_offset), u.curr_aspect, u.view_aspect);
        col_curr.r = sample_image(t_current, s_sampler, uv_ca_r).r;
        col_curr.b = sample_image(t_current, s_sampler, uv_ca_b).b;

        // Shockwave expansion edge and tear boundary
        let tear_edge = blast_radius + angle_noise * 0.15;
        let is_blown_open = smoothstep(tear_edge - 0.08, tear_edge + 0.08, effective_radius);

        // Shockwave glow at the blast wavefront
        let wave_thickness = 0.12 * (1.0 - t * 0.5);
        let shockwave = smoothstep(wave_thickness, 0.0, abs(effective_radius - blast_radius)) * (1.0 - t * 0.8) * 0.85;
        let center_flash = smoothstep(0.25, 0.0, t) * max(1.0 - r * 2.5, 0.0) * 0.5;

        // Incoming image zooms slightly into place behind the blast
        let next_zoom = mix(0.92, 1.0, smoothstep(0.0, 0.9, t));
        let next_viewport_uv = (uv - center) / max(next_zoom, 0.001) + center;
        let uv_next = fit_uv(next_viewport_uv, u.next_aspect, u.view_aspect);
        let col_next = sample_image(t_next, s_sampler, uv_next);

        // Combine: incoming image fills inside cavity, blown-out pixels and shockwave outside
        let col_exploded = col_curr + vec4<f32>(vec3<f32>(shockwave + center_flash), 0.0);
        let fade_edge = smoothstep(0.0, 1.0, is_blown_open * (1.0 - blast_t * 0.9));

        return mix(col_next, col_exploded, fade_edge);
    }

    // Default Crossfade
    let uv1 = fit_uv(uv, u.curr_aspect, u.view_aspect);
    let uv2 = fit_uv(uv, u.next_aspect, u.view_aspect);
    let c1 = sample_image(t_current, s_sampler, uv1);
    let c2 = sample_image(t_next, s_sampler, uv2);
    return mix(c1, c2, smoothstep(0.0, 1.0, t));
}
"#;

pub struct SlideshowPipeline {
    pub pipeline: Arc<wgpu::RenderPipeline>,
    pub bind_group_layout: wgpu::BindGroupLayout,
    pub sampler: wgpu::Sampler,
    pub uniform_buffer: wgpu::Buffer,
    pub fallback_texture: wgpu::Texture,
    pub fallback_view: wgpu::TextureView,
}

impl SlideshowPipeline {
    pub fn new(device: &wgpu::Device, target_format: wgpu::TextureFormat) -> Self {
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("slideshow_shader"),
            source: wgpu::ShaderSource::Wgsl(SLIDESHOW_SHADER_WGSL.into()),
        });

        let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("slideshow_bind_group_layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Texture {
                        sample_type: wgpu::TextureSampleType::Float { filterable: true },
                        view_dimension: wgpu::TextureViewDimension::D2,
                        multisampled: false,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 3,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Texture {
                        sample_type: wgpu::TextureSampleType::Float { filterable: true },
                        view_dimension: wgpu::TextureViewDimension::D2,
                        multisampled: false,
                    },
                    count: None,
                },
            ],
        });

        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("slideshow_pipeline_layout"),
            bind_group_layouts: &[Some(&bind_group_layout)],
            immediate_size: 0,
        });

        let pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("slideshow_render_pipeline"),
            layout: Some(&pipeline_layout),
            vertex: wgpu::VertexState {
                module: &shader,
                entry_point: Some("vs_main"),
                buffers: &[],
                compilation_options: Default::default(),
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: Some("fs_main"),
                targets: &[Some(wgpu::ColorTargetState {
                    format: target_format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: wgpu::PrimitiveState {
                topology: wgpu::PrimitiveTopology::TriangleList,
                strip_index_format: None,
                front_face: wgpu::FrontFace::Ccw,
                cull_mode: None,
                unclipped_depth: false,
                polygon_mode: wgpu::PolygonMode::Fill,
                conservative: false,
            },
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            multiview_mask: None,
            cache: None,
        });

        let sampler = device.create_sampler(&wgpu::SamplerDescriptor {
            label: Some("slideshow_sampler"),
            address_mode_u: wgpu::AddressMode::ClampToEdge,
            address_mode_v: wgpu::AddressMode::ClampToEdge,
            address_mode_w: wgpu::AddressMode::ClampToEdge,
            mag_filter: wgpu::FilterMode::Linear,
            min_filter: wgpu::FilterMode::Linear,
            mipmap_filter: wgpu::MipmapFilterMode::Linear,
            ..Default::default()
        });

        let uniform_buffer = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("slideshow_uniform_buffer"),
            size: std::mem::size_of::<SlideshowUniforms>() as u64,
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let fallback_texture = device.create_texture(&wgpu::TextureDescriptor {
            label: Some("slideshow_fallback_texture"),
            size: wgpu::Extent3d { width: 1, height: 1, depth_or_array_layers: 1 },
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rgba8Unorm,
            usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
            view_formats: &[],
        });
        let fallback_view = fallback_texture.create_view(&wgpu::TextureViewDescriptor::default());

        Self {
            pipeline: Arc::new(pipeline),
            bind_group_layout,
            sampler,
            uniform_buffer,
            fallback_texture,
            fallback_view,
        }
    }
}

pub struct SlideshowManager {
    pub pipeline: SlideshowPipeline,
    pub effect_choice: SlideshowEffectChoice,
    pub current_effect: SlideshowEffect,
    pub transition_start: Option<Instant>,
    pub transition_duration: Duration,
    pub prev_path: Option<PathBuf>,
    pub current_path: Option<PathBuf>,
    pub uniforms: SlideshowUniforms,
    pub texture_cache: HashMap<PathBuf, (wgpu::Texture, wgpu::TextureView)>,
    pub texture_dimensions: HashMap<PathBuf, (u32, u32)>,
}

impl SlideshowManager {
    pub fn new(
        device: &wgpu::Device,
        target_format: wgpu::TextureFormat,
        choice: SlideshowEffectChoice,
    ) -> Self {
        let pipeline = SlideshowPipeline::new(device, target_format);
        Self {
            pipeline,
            effect_choice: choice,
            current_effect: choice.pick_effective(None),
            transition_start: None,
            transition_duration: Duration::from_millis(1200),
            prev_path: None,
            current_path: None,
            uniforms: SlideshowUniforms::default(),
            texture_cache: HashMap::new(),
            texture_dimensions: HashMap::new(),
        }
    }

    pub fn set_effect_choice(&mut self, choice: SlideshowEffectChoice) {
        self.effect_choice = choice;
        self.current_effect = choice.pick_effective(Some(self.current_effect));
    }

    pub fn on_slide_change(&mut self, from_path: Option<PathBuf>, to_path: Option<PathBuf>) {
        self.prev_path = from_path;
        self.current_path = to_path;
        self.current_effect = self.effect_choice.pick_effective(Some(self.current_effect));
        self.transition_start = Some(Instant::now());

        let zoom_in = random_u64().is_multiple_of(2);
        let (z_start, z_end) =
            if zoom_in { (1.0, random_f32(1.15, 1.25)) } else { (random_f32(1.15, 1.25), 1.0) };

        let px_start = random_f32(-0.04, 0.04);
        let py_start = random_f32(-0.04, 0.04);
        let px_end = random_f32(-0.04, 0.04);
        let py_end = random_f32(-0.04, 0.04);

        let direction = if random_u64().is_multiple_of(2) { 1.0 } else { -1.0 };

        self.uniforms.effect_type = self.current_effect.effect_type_id();
        self.uniforms.direction = direction;
        self.uniforms.param1 = z_start;
        self.uniforms.param2 = z_end;
        self.uniforms.param3 = px_start;
        self.uniforms.param4 = py_start;
        self.uniforms.param5 = px_end;
        self.uniforms.param6 = py_end;
    }

    pub fn register_color_image(
        &mut self,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        path: &PathBuf,
        image: &egui::ColorImage,
    ) {
        let width = image.size[0] as u32;
        let height = image.size[1] as u32;

        let size =
            wgpu::Extent3d { width: width.max(1), height: height.max(1), depth_or_array_layers: 1 };

        let texture = device.create_texture(&wgpu::TextureDescriptor {
            label: Some(&format!("slideshow_tex_{:?}", path.file_name())),
            size,
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rgba8Unorm,
            usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
            view_formats: &[],
        });

        queue.write_texture(
            texture.as_image_copy(),
            image.as_raw(),
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(4 * width.max(1)),
                rows_per_image: Some(height.max(1)),
            },
            size,
        );

        let view = texture.create_view(&wgpu::TextureViewDescriptor::default());
        self.texture_cache.insert(path.clone(), (texture, view));
        self.texture_dimensions.insert(path.clone(), (width, height));
    }

    pub fn register_deep_image(
        &mut self,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        path: &PathBuf,
        pixels: &super::image::DeepPixels,
        width: u32,
        height: u32,
    ) {
        let width = width.max(1);
        let height = height.max(1);

        let (raw_bytes, bpp, format) = match pixels {
            super::image::DeepPixels::Rgb10a2(v) => (
                bytemuck::cast_slice::<u32, u8>(v.as_slice()),
                4,
                wgpu::TextureFormat::Rgb10a2Unorm,
            ),
            super::image::DeepPixels::Rgba16(v) => {
                (bytemuck::cast_slice::<u16, u8>(v.as_slice()), 8, wgpu::TextureFormat::Rgba16Unorm)
            }
        };

        let size = wgpu::Extent3d { width, height, depth_or_array_layers: 1 };

        let texture = device.create_texture(&wgpu::TextureDescriptor {
            label: Some(&format!("slideshow_deep_tex_{:?}", path.file_name())),
            size,
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format,
            usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
            view_formats: &[],
        });

        queue.write_texture(
            texture.as_image_copy(),
            raw_bytes,
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(bpp * width),
                rows_per_image: Some(height),
            },
            size,
        );

        let view = texture.create_view(&wgpu::TextureViewDescriptor::default());
        self.texture_cache.insert(path.clone(), (texture, view));
        self.texture_dimensions.insert(path.clone(), (width, height));
    }

    pub fn prune_cache(&mut self, active_paths: &std::collections::HashSet<PathBuf>) {
        self.texture_cache.retain(|k, _| active_paths.contains(k));
        self.texture_dimensions.retain(|k, _| active_paths.contains(k));
    }

    pub fn is_transition_active(&self) -> bool {
        if let Some(start) = self.transition_start {
            start.elapsed() < self.transition_duration
        } else {
            false
        }
    }

    pub fn render(
        &mut self,
        queue: &wgpu::Queue,
        device: &wgpu::Device,
        ui: &mut egui::Ui,
        rect: egui::Rect,
    ) {
        let progress = if let Some(start) = self.transition_start {
            (start.elapsed().as_secs_f32() / self.transition_duration.as_secs_f32()).clamp(0.0, 1.0)
        } else {
            1.0
        };

        self.uniforms.progress = progress;
        self.uniforms.view_aspect = (rect.width() / rect.height().max(1.0)).max(0.01);

        let curr_aspect = self
            .prev_path
            .as_ref()
            .and_then(|p| self.texture_dimensions.get(p))
            .map(|(w, h)| *w as f32 / (*h as f32).max(1.0))
            .unwrap_or(self.uniforms.view_aspect);

        let next_aspect = self
            .current_path
            .as_ref()
            .and_then(|p| self.texture_dimensions.get(p))
            .map(|(w, h)| *w as f32 / (*h as f32).max(1.0))
            .unwrap_or(curr_aspect);

        self.uniforms.curr_aspect = curr_aspect;
        self.uniforms.next_aspect = next_aspect;

        queue.write_buffer(&self.pipeline.uniform_buffer, 0, bytemuck::bytes_of(&self.uniforms));

        let fallback = &self.pipeline.fallback_view;
        let view_from = self
            .prev_path
            .as_ref()
            .and_then(|p| self.texture_cache.get(p))
            .map(|(_, v)| v)
            .unwrap_or(fallback);

        let view_to = self
            .current_path
            .as_ref()
            .and_then(|p| self.texture_cache.get(p))
            .map(|(_, v)| v)
            .unwrap_or(view_from);

        let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("slideshow_active_bind_group"),
            layout: &self.pipeline.bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: self.pipeline.uniform_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: wgpu::BindingResource::Sampler(&self.pipeline.sampler),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: wgpu::BindingResource::TextureView(view_from),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: wgpu::BindingResource::TextureView(view_to),
                },
            ],
        });

        let callback = egui_wgpu::Callback::new_paint_callback(
            rect,
            SlideshowPaintCallback {
                pipeline: self.pipeline.pipeline.clone(),
                bind_group: Arc::new(bind_group),
            },
        );

        ui.painter().add(callback);
    }
}

struct SlideshowPaintCallback {
    pipeline: Arc<wgpu::RenderPipeline>,
    bind_group: Arc<wgpu::BindGroup>,
}

impl egui_wgpu::CallbackTrait for SlideshowPaintCallback {
    fn paint(
        &self,
        _info: egui::PaintCallbackInfo,
        render_pass: &mut wgpu::RenderPass<'static>,
        _callback_resources: &egui_wgpu::CallbackResources,
    ) {
        render_pass.set_pipeline(&self.pipeline);
        render_pass.set_bind_group(0, &*self.bind_group, &[]);
        render_pass.draw(0..6, 0..1);
    }
}
