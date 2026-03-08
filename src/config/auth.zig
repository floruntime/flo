//! Authentication Configuration
//!
//! Configuration types for JWT-based authentication.
//! Supports HS256 (shared secret) and RS256 (JWKS public keys).

/// Authentication configuration for JWT validation.
///
/// Algorithm selection:
/// - If `jwt_secret` is set: HS256 (HMAC-SHA256 with shared secret)
/// - If `jwks_url` is set: RS256 (RSASSA-PKCS1-v1_5 with JWKS public keys)
/// - If both are set: HS256 is tried first, RS256 as fallback
pub const AuthServerConfig = struct {
    /// Enable authentication (if false, all connections are anonymous)
    enabled: bool = false,
    /// JWT secret for HS256 verification (base64 encoded or raw string)
    jwt_secret: ?[]const u8 = null,
    /// JWKS URL for RS256 verification (fetched and cached with 1h TTL)
    jwks_url: ?[]const u8 = null,
};
