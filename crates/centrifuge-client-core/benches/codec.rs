//! Codec, stream reassembly and delta benchmarks (docs/spec.md §11.1).
//!
//! `cargo bench -p centrifuge-client-core`

#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)] // benchmarks

use bytes::Bytes;
use centrifuge_client_core::codec::{self, Protocol, StreamDecoder};
use centrifuge_client_core::{delta, proto};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use prost::Message as _;

const MAX: usize = 64 << 20;

fn payload() -> String {
    // A typical small JSON document.
    r#"{"id":12345,"user":"alice","text":"Hello, this is a chat message of ordinary length.","tags":["a","b"]}"#.to_owned()
}

fn pub_reply(offset: u64) -> proto::Reply {
    proto::Reply {
        push: Some(proto::Push {
            channel: "chat:index".into(),
            r#pub: Some(proto::Publication {
                offset,
                data: Bytes::from(payload()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn json_line(offset: u64) -> String {
    format!(
        r#"{{"push":{{"channel":"chat:index","pub":{{"data":{},"offset":{offset}}}}}}}"#,
        payload()
    )
}

fn protobuf_frame(n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    for o in 1..=n {
        pub_reply(o).encode_length_delimited(&mut out).expect("vec");
    }
    out
}

fn json_frame(n: u64) -> Vec<u8> {
    (1..=n)
        .map(json_line)
        .collect::<Vec<_>>()
        .join("\n")
        .into_bytes()
}

fn decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("decode");
    for n in [1u64, 100] {
        let pb = Bytes::from(protobuf_frame(n));
        let js = Bytes::from(json_frame(n));
        g.throughput(Throughput::Elements(n));
        g.bench_with_input(BenchmarkId::new("protobuf", n), &pb, |b, f| {
            b.iter(|| codec::decode_frame(Protocol::Protobuf, f.clone(), MAX).expect("decodes"));
        });
        g.bench_with_input(BenchmarkId::new("json", n), &js, |b, f| {
            b.iter(|| codec::decode_frame(Protocol::Json, f.clone(), MAX).expect("decodes"));
        });
    }
    g.finish();
}

fn encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("encode");
    let cmds: Vec<proto::Command> = (1..=50)
        .map(|i| proto::Command {
            id: i,
            subscribe: Some(proto::SubscribeRequest {
                channel: format!("channel:{i}"),
                recoverable: true,
                ..Default::default()
            }),
            ..Default::default()
        })
        .collect();
    let publish = vec![proto::Command {
        id: 1,
        publish: Some(proto::PublishRequest {
            channel: "chat:index".into(),
            data: Bytes::from(payload()),
            ..Default::default()
        }),
        ..Default::default()
    }];
    for (name, protocol) in [("protobuf", Protocol::Protobuf), ("json", Protocol::Json)] {
        g.bench_function(BenchmarkId::new("publish", name), |b| {
            let mut out = Vec::with_capacity(256);
            b.iter(|| {
                out.clear();
                codec::encode_commands(protocol, &publish, &mut out).expect("encodes");
            });
        });
        g.bench_function(BenchmarkId::new("50-subscribes", name), |b| {
            let mut out = Vec::with_capacity(4096);
            b.iter(|| {
                out.clear();
                codec::encode_commands(protocol, &cmds, &mut out).expect("encodes");
            });
        });
    }
    g.finish();
}

/// A byte stream (HTTP-streaming) split into small reads.
fn reassembly(c: &mut Criterion) {
    let mut g = c.benchmark_group("stream-reassembly");
    let pb = protobuf_frame(500);
    let mut js = json_frame(500);
    js.push(b'\n');
    for (name, protocol, data) in [
        ("protobuf", Protocol::Protobuf, pb),
        ("json", Protocol::Json, js),
    ] {
        g.throughput(Throughput::Bytes(data.len() as u64));
        for chunk in [64usize, 1460] {
            g.bench_function(BenchmarkId::new(name, chunk), |b| {
                b.iter(|| {
                    let mut d = StreamDecoder::new(protocol, MAX);
                    let mut out = Vec::new();
                    for c in data.chunks(chunk) {
                        d.push(c, &mut out).expect("decodes");
                    }
                    assert_eq!(out.len(), 500);
                });
            });
        }
        // A single large message in small reads (quadratic copying shows here).
        let big = {
            let r = proto::Reply {
                push: Some(proto::Push {
                    channel: "c".into(),
                    r#pub: Some(proto::Publication {
                        offset: 1,
                        data: Bytes::from(vec![b'x'; 1 << 20]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let mut v = Vec::new();
            r.encode_length_delimited(&mut v).expect("vec");
            v
        };
        if protocol == Protocol::Protobuf {
            g.throughput(Throughput::Bytes(big.len() as u64));
            g.bench_function(
                BenchmarkId::new("1MiB-message-in-1460-byte-reads", name),
                |b| {
                    b.iter(|| {
                        let mut d = StreamDecoder::new(protocol, MAX);
                        let mut out = Vec::new();
                        for c in big.chunks(1460) {
                            d.push(c, &mut out).expect("decodes");
                        }
                        assert_eq!(out.len(), 1);
                    });
                },
            );
        }
    }
    g.finish();
}

fn fossil(c: &mut Criterion) {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/fossil-vectors");
    let mut g = c.benchmark_group("delta");
    for case in ["1", "2", "3", "4", "5"] {
        let p = dir.join(case);
        let (Ok(origin), Ok(d)) = (
            std::fs::read(p.join("origin")),
            std::fs::read(p.join("delta")),
        ) else {
            continue;
        };
        g.throughput(Throughput::Bytes(origin.len() as u64));
        g.bench_function(BenchmarkId::new("apply", case), |b| {
            b.iter(|| delta::apply(&origin, &d, MAX).expect("applies"));
        });
    }
    g.finish();
}

criterion_group!(benches, decode, encode, reassembly, fossil);
criterion_main!(benches);
