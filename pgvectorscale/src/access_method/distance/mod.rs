//! Distance functions backed by hanns SIMD kernels.

use pgrx::pg_extern;

pub type DistanceFn = fn(&[f32], &[f32]) -> f32;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DistanceType {
    Cosine = 0,
    L2 = 1,
    InnerProduct = 2,
}

impl DistanceType {
    pub fn from_u16(value: u16) -> Self {
        match value {
            0 => DistanceType::Cosine,
            1 => DistanceType::L2,
            2 => DistanceType::InnerProduct,
            _ => panic!("Unknown DistanceType number {}", value),
        }
    }

    pub fn get_operator(&self) -> &str {
        match self {
            DistanceType::Cosine => "<=>",
            DistanceType::L2 => "<->",
            DistanceType::InnerProduct => "<#>",
        }
    }

    pub fn get_operator_class(&self) -> &str {
        match self {
            DistanceType::Cosine => "vector_cosine_ops",
            DistanceType::L2 => "vector_l2_ops",
            DistanceType::InnerProduct => "vector_ip_ops",
        }
    }

    pub fn get_distance_function(&self) -> DistanceFn {
        match self {
            DistanceType::Cosine => distance_cosine,
            DistanceType::L2 => distance_l2,
            DistanceType::InnerProduct => distance_inner_product,
        }
    }

    pub fn to_hanns_metric(&self) -> hanns::api::MetricType {
        match self {
            DistanceType::Cosine => hanns::api::MetricType::Cosine,
            DistanceType::L2 => hanns::api::MetricType::L2,
            DistanceType::InnerProduct => hanns::api::MetricType::Ip,
        }
    }
}

#[pg_extern(immutable, parallel_safe, create_or_replace)]
pub fn distance_type_cosine() -> i16 {
    DistanceType::Cosine as i16
}

#[pg_extern(immutable, parallel_safe, create_or_replace)]
pub fn distance_type_l2() -> i16 {
    DistanceType::L2 as i16
}

#[pg_extern(immutable, parallel_safe, create_or_replace)]
pub fn distance_type_inner_product() -> i16 {
    DistanceType::InnerProduct as i16
}

pub fn init() {
    // hanns uses runtime CPU detection, no compile-time requirement
}

/// Squared L2 distance using hanns SIMD kernels.
#[inline]
pub fn distance_l2(a: &[f32], b: &[f32]) -> f32 {
    hanns::simd::l2_sq(a, b)
}

/// Negative inner product (distance convention) using hanns SIMD kernels.
#[inline]
pub fn distance_inner_product(a: &[f32], b: &[f32]) -> f32 {
    -hanns::simd::dot_product_f32(a, b)
}

/// Cosine distance using hanns SIMD kernels.
#[inline]
pub fn distance_cosine(a: &[f32], b: &[f32]) -> f32 {
    let ip = hanns::simd::dot_product_f32(a, b);
    let norm_a: f32 = a.iter().map(|v| v * v).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    (1.0 - ip / (norm_a * norm_b)).max(0.0)
}

/// Preprocess vector for cosine search: normalize to unit length.
pub fn preprocess_cosine(a: &mut [f32]) {
    let norm = a.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        a.iter_mut().for_each(|v| *v /= norm);
    }
}
