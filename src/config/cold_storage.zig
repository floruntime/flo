//! Cold Storage Configuration
//!
//! Configuration types for cold storage backends (S3, file, etc.)

const std = @import("std");

/// Cold storage provider type
pub const ColdStorageProvider = enum {
    /// No cold storage (segments stay local forever)
    none,
    /// Local filesystem / NFS / SAN
    file,
    /// AWS S3 or S3-compatible (MinIO, R2, DigitalOcean)
    s3,

    pub fn fromString(s: []const u8) ColdStorageProvider {
        if (std.mem.eql(u8, s, "none") or std.mem.eql(u8, s, "noop")) return .none;
        if (std.mem.eql(u8, s, "file") or std.mem.eql(u8, s, "local")) return .file;
        if (std.mem.eql(u8, s, "s3") or std.mem.eql(u8, s, "aws")) return .s3;
        return .none; // default
    }
};

/// File backend configuration (local filesystem or NFS mount)
pub const FileConfig = struct {
    /// Base path for archived segments
    base_path: []const u8 = "/var/lib/flo/archive",
    /// Create directories if they don't exist
    create_dirs: bool = true,
    /// Sync file to disk after write (default: true)
    sync_on_write: bool = true,
};

/// S3 backend configuration
pub const S3Config = struct {
    /// S3 bucket name (required if using S3)
    bucket: []const u8 = "",
    /// Key prefix for all objects (optional, for multi-tenant)
    prefix: []const u8 = "",
    /// AWS region (default: us-east-1)
    region: []const u8 = "us-east-1",
    /// Custom endpoint URL (for MinIO, R2, etc.)
    endpoint: ?[]const u8 = null,
    /// Access key ID (or use env AWS_ACCESS_KEY_ID)
    access_key_id: ?[]const u8 = null,
    /// Secret access key (or use env AWS_SECRET_ACCESS_KEY)
    secret_access_key: ?[]const u8 = null,
    /// AWS session token (for temporary credentials)
    session_token: ?[]const u8 = null,
    /// Use path-style URLs (required for some S3-compatible services)
    use_path_style: bool = false,
    /// Use IAM role for credentials (EC2/ECS/EKS)
    use_iam_role: bool = true,
    /// Multipart upload threshold (files larger than this use multipart)
    multipart_threshold: usize = 100 * 1024 * 1024, // 100MB
    /// Multipart part size
    multipart_part_size: usize = 16 * 1024 * 1024, // 16MB
    /// Connection timeout in milliseconds
    connect_timeout_ms: u32 = 5000,
    /// Request timeout in milliseconds
    request_timeout_ms: u32 = 30000,
    /// Enable TLS/SSL (should always be true for production)
    use_tls: bool = true,
    /// Maximum concurrent uploads
    max_concurrent_uploads: u8 = 4,
};

/// Cold storage configuration
/// Supports file and S3 backends with nested config structs
pub const ColdStorageConfig = struct {
    /// Provider type (none, file, s3)
    provider: ColdStorageProvider = .none,

    /// Number of upload worker threads (default: 2)
    upload_workers: u8 = 2,
    /// Number of restore worker threads (default: 4)
    restore_workers: u8 = 4,
    /// Verify checksums after upload/download
    verify_checksums: bool = true,
    /// Enable compression (zstd)
    compression_enabled: bool = true,

    /// Time-based retention in days (default: 7)
    /// Segments uploaded to cold storage are kept locally for this duration
    hot_retention_days: u32 = 7,
    /// Disk usage high watermark (default: 80%)
    /// When exceeded, retention is reduced to 1/4
    disk_high_watermark: f32 = 0.80,
    /// Disk usage critical watermark (default: 90%)
    /// When exceeded, evict immediately after upload
    disk_critical_watermark: f32 = 0.90,

    /// Minimum batch size for uploads (bytes, default: 10MB)
    /// Set to 0 to disable batching
    min_upload_batch_size: usize = 10 * 1024 * 1024,
    /// Maximum batch age (seconds, default: 300)
    max_upload_batch_age_seconds: u64 = 300,

    /// File backend configuration (used when provider = .file)
    file: FileConfig = .{ .base_path = "/var/lib/flo/archive" },

    /// S3 backend configuration (used when provider = .s3)
    s3: S3Config = .{ .bucket = "" },
};
