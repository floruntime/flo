//! Two directories below its test root on purpose: the generated root's
//! self-check requires this test by name, so a test-root walk that finds
//! nothing, or doesn't descend into subdirectories, fails the build's tests
//! instead of quietly running fewer.

test "test root sentinel" {}
