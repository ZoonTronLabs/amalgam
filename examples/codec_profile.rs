//! Repeatable codec microbenchmark; compile first, then run without other workloads.
use std::hint::black_box;
use std::time::{Duration, Instant};

use amalgam::entry::Entry;
use amalgam::{
    DistributedEntry, DistributedSerializer, DistributedSnapshot, EntryOptions, EntryWeight,
    JitterSample, JsonSerializer, Priority, SnapshotRetention, Timestamp,
};

type Codec = (&'static str, Box<dyn DistributedSerializer<Vec<u8>>>);

fn codecs() -> Vec<Codec> {
    vec![
        ("json", Box::new(JsonSerializer)),
        #[cfg(feature = "messagepack")]
        ("messagepack", Box::new(amalgam::MessagePackSerializer)),
        #[cfg(feature = "postcard")]
        ("postcard", Box::new(amalgam::PostcardSerializer)),
    ]
}

fn snapshot(size: usize) -> amalgam::Result<DistributedSnapshot<Vec<u8>>> {
    let at = Timestamp::from_ticks(1_000_000_000);
    let options = EntryOptions::new(Duration::from_secs(60));
    let entry = Entry::try_fresh_with_jitter(
        vec![7; size],
        &options,
        at,
        at,
        JitterSample::new(Duration::ZERO, Duration::ZERO)?,
        Box::new([]),
        None,
        None,
    )?;
    DistributedSnapshot::new(
        DistributedEntry::from_entry(&entry),
        at,
        SnapshotRetention::Specified {
            size: Some(EntryWeight::new(size as u64)),
            priority: Priority::Normal,
        },
    )
}

#[derive(Clone, Copy)]
enum Wire {
    Legacy,
    V2,
}

impl Wire {
    fn label(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::V2 => "v2",
        }
    }
    fn encode(
        self,
        codec: &dyn DistributedSerializer<Vec<u8>>,
        snapshot: &DistributedSnapshot<Vec<u8>>,
    ) -> amalgam::Result<Vec<u8>> {
        match self {
            Self::Legacy => codec.serialize(snapshot.entry()),
            Self::V2 => codec.serialize_snapshot(snapshot),
        }
    }
    fn decode(
        self,
        codec: &dyn DistributedSerializer<Vec<u8>>,
        bytes: &[u8],
    ) -> amalgam::Result<()> {
        match self {
            Self::Legacy => {
                black_box(codec.deserialize(bytes)?);
            }
            Self::V2 => {
                black_box(codec.deserialize_snapshot(bytes)?);
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("codec,wire,input_bytes,round,operations,encoded_bytes,encode_ns,decode_ns");
    for (size, operations) in [(1024, 2000), (65_536, 500), (1_048_576, 30)] {
        let snapshot = snapshot(size)?;
        for (name, codec) in codecs() {
            for wire in [Wire::Legacy, Wire::V2] {
                let bytes = wire.encode(codec.as_ref(), &snapshot)?;
                match wire {
                    Wire::Legacy => {
                        assert_eq!(codec.deserialize(&bytes)?.value, snapshot.entry().value)
                    }
                    Wire::V2 => assert_eq!(
                        codec.deserialize_snapshot(&bytes)?.entry().value,
                        snapshot.entry().value
                    ),
                }
                for _ in 0..30 {
                    black_box(wire.encode(codec.as_ref(), &snapshot)?);
                    wire.decode(codec.as_ref(), &bytes)?;
                }
                for round in 0..5 {
                    let start = Instant::now();
                    for _ in 0..operations {
                        black_box(wire.encode(codec.as_ref(), &snapshot)?);
                    }
                    let encode_ns = start.elapsed().as_nanos() as f64 / operations as f64;
                    let start = Instant::now();
                    for _ in 0..operations {
                        wire.decode(codec.as_ref(), &bytes)?;
                    }
                    let decode_ns = start.elapsed().as_nanos() as f64 / operations as f64;
                    println!(
                        "{name},{},{size},{round},{operations},{},{encode_ns:.3},{decode_ns:.3}",
                        wire.label(),
                        bytes.len()
                    );
                }
            }
        }
    }
    Ok(())
}
