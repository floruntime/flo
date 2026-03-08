//! Authentication Configuration
//!
//! Configuration types for JWT-based authentication.
//! Supports HS256 (shared secret), RS256 (RSA via JWKS), and ES256 (ECDSA P-256 via JWKS).

/// Authentication configuration for JWT validation.
///
/// Algorithm selection:
/// - If `jwt_secret` is set: HS256 (HMAC-SHA256 with shared secret)
/// - If `jwks_url` is set: RS256 or ES256 (auto-detected from JWT header, keys from JWKS)
/// - If both are set: HS256 is tried first, JWKS-based verification as fallback
///
/// Provider examples:
/// - Supabase: uses ES256 (ECC P-256), set `jwks_url` to project JWKS endpoint
/// - Auth0/Okta/Azure AD: use RS256 (RSA), set `jwks_url` to provider JWKS endpoint
/// - Custom/simple: use HS256, set `jwt_secret` to shared secret
pub const AuthServerConfig = struct {
    /// Enable authentication (if false, all connections are anonymous)
    enabled: bool = false,
    /// JWT secret for HS256 verification (base64 encoded or raw string)
    jwt_secret: ?[]const u8 = null,
    /// JWKS URL for RS256 verification (fetched and cached with 1h TTL)
    jwks_url: ?[]const u8 = null,
};
