//! Compile the exact production primitive with cfg(loom) on this final target.
//! Dependencies retain their own ordinary cfg so their unrelated optional
//! Loom integrations cannot replace or break this private primitive's model.
#![cfg(loom)]
#![deny(unsafe_code)]

#[allow(unsafe_code)]
#[path = "../src/reader_slots.rs"]
mod reader_slots;
