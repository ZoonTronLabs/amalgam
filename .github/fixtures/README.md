# Downstream package fixtures

`package-consumer.rs` is compiled and run against the extracted `.crate` by `check-package.sh`, with default features and `full`. It exercises fallible public reads and mutations, L2 read-through and explicit shutdown.

`consumer-Cargo.lock` is an immutable historic resolver input from Amalgam commit `c82c2c71f7960751c1c2836a61dfb1dd3b3249fb`, before the dependency security-floor repair. It is not a runtime or publication lock. The historic consumer must re-resolve to patched dependencies from the new package manifest and pass the current RustSec audit; updating only the library's own Cargo.lock is insufficient.

Both a fresh consumer and this historic-lock consumer are required. These fixtures are excluded from the published package with the rest of `.github`.
