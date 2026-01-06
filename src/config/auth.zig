//! Authentication Configuration
//!
//! Configuration types for JWT-based authentication

/// Authentication configuration for JWT validation
pub const AuthServerConfig = struct {
    /// Enable authentication (if false, all connections are anonymous)
    enabled: bool = false,
    /// JWT secret for HS256 verification (base64 encoded or raw string)
    jwt_secret: ?[]const u8 = null,
    /// JWKS URL for RS256 verification (not yet implemented)
    jwks_url: ?[]const u8 = null,
};
