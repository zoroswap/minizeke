//! Standalone dynamic-fee policy and transport adapters.
//!
//! This module deliberately has no dependencies on the rest of Minizeke so it
//! can be wired into either an in-process keeper or a separate keeper binary.

pub mod config;
pub mod curve;
pub mod http;
pub mod oracle;
pub mod policy;
pub mod volatility;

pub use config::{
    Config, FeeCurveConfig, HttpConfig, OracleConfig, UpdatePolicyConfig, VolatilityConfig,
};
pub use curve::{FeeCurve, FeeCurveError};
pub use http::{
    BatchFeeUpdateRequest, BatchFeeUpdateResponse, FeeSource, FeeUpdate, MinizekeFeeClient,
};
pub use oracle::{OracleClient, OracleError, OracleSample};
pub use policy::{Evaluation, FeePolicy};
pub use volatility::{VolatilityError, VolatilityEstimator, VolatilitySnapshot};

/// Minizeke fee precision is one million, so one basis point is 100 units.
pub const FEE_UNITS_PER_BPS: f64 = 100.0;
