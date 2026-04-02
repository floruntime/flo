//! Kafka Client Module
//!
//! Pure-Zig Kafka wire protocol client for KafkaSource integration.
//! Phase 1: plaintext + SASL/PLAIN, JSON + RAW + STRING, GZIP compression.
//! Phase 2: SCRAM-SHA-256/512, Snappy/LZ4/ZSTD compression.
//! Phase 3: Schema Registry, Avro, Protobuf, AWS MSK IAM, TLS/mTLS.
//!
//! Public imports for use by the processing handler and other modules.

pub const codec = @import("codec.zig");
pub const protocol = @import("protocol.zig");
pub const broker = @import("broker.zig");
pub const auth = @import("auth.zig");
pub const record_batch = @import("record_batch.zig");
pub const compress = @import("compress.zig");
pub const deser = @import("deser.zig");
pub const source = @import("source.zig");
pub const schema_registry = @import("schema_registry.zig");
pub const avro_decoder = @import("avro.zig");
pub const protobuf_decoder = @import("protobuf.zig");
pub const msk_iam = @import("msk_iam.zig");
pub const tls_transport = @import("tls.zig");

pub const KafkaSource = source.KafkaSource;
pub const KafkaSourceConfig = source.KafkaSourceConfig;
pub const BrokerPool = broker.BrokerPool;
pub const BrokerAddress = broker.BrokerAddress;
pub const Deserializer = deser.Deserializer;
pub const SchemaRegistryClient = schema_registry.SchemaRegistryClient;
pub const ErrorCode = protocol.ErrorCode;

test {
    _ = codec;
    _ = protocol;
    _ = broker;
    _ = auth;
    _ = record_batch;
    _ = compress;
    _ = deser;
    _ = source;
    _ = schema_registry;
    _ = avro_decoder;
    _ = protobuf_decoder;
    _ = msk_iam;
    _ = tls_transport;
}
