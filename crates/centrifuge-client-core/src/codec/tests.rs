use bytes::Bytes;

use super::*;
use crate::proto;

const MAX: usize = 1 << 20;

fn sub_cmd(id: u32, channel: &str) -> proto::Command {
    proto::Command {
        id,
        subscribe: Some(proto::SubscribeRequest {
            channel: channel.into(),
            recover: true,
            offset: 5,
            epoch: "e".into(),
            data: Bytes::from_static(br#"{"x":1}"#),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(feature = "json")]
mod json {
    use super::*;

    fn encode(cmds: &[proto::Command]) -> String {
        let mut out = Vec::new();
        encode_commands(Protocol::Json, cmds, &mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn encodes_exact_field_names_and_omits_defaults() {
        let s = encode(&[sub_cmd(1, "chat")]);
        assert_eq!(
            s,
            r#"{"id":1,"subscribe":{"channel":"chat","recover":true,"epoch":"e","offset":5,"data":{"x":1}}}"#
        );
    }

    #[test]
    fn pong_is_an_empty_object_and_batches_join_with_newline() {
        let s = encode(&[proto::Command::default(), sub_cmd(2, "a")]);
        let mut lines = s.split('\n');
        assert_eq!(lines.next(), Some("{}"));
        assert!(lines.next().unwrap().starts_with(r#"{"id":2,"#));
        assert!(!s.ends_with('\n'));
    }

    #[test]
    fn invalid_json_payload_is_rejected_and_nothing_appended() {
        let mut cmd = sub_cmd(1, "c");
        cmd.subscribe.as_mut().unwrap().data = Bytes::from_static(b"not json");
        let mut out = b"keep".to_vec();
        let err = encode_commands(Protocol::Json, &[cmd], &mut out).unwrap_err();
        assert!(matches!(err, EncodeError::InvalidJson { .. }));
        assert_eq!(out, b"keep");
    }

    #[test]
    fn connect_with_subs_headers_and_filter() {
        let mut subs = std::collections::HashMap::new();
        subs.insert(
            "s".to_string(),
            proto::SubscribeRequest {
                recover: true,
                offset: 3,
                epoch: "x".into(),
                ..Default::default()
            },
        );
        let mut headers = std::collections::HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer t".to_string());
        let cmd = proto::Command {
            id: 1,
            connect: Some(proto::ConnectRequest {
                token: "tok".into(),
                name: "rust".into(),
                subs,
                headers,
                ..Default::default()
            }),
            ..Default::default()
        };
        let v: serde_json::Value = serde_json::from_str(&encode(&[cmd])).unwrap();
        assert_eq!(v["connect"]["subs"]["s"]["offset"], 3);
        assert_eq!(v["connect"]["headers"]["Authorization"], "Bearer t");
        assert!(v["connect"].get("data").is_none());

        let f = crate::filter::Filter::and([crate::filter::Filter::eq("k", "v")]);
        let cmd = proto::Command {
            id: 2,
            subscribe: Some(proto::SubscribeRequest {
                channel: "c".into(),
                tf: Some(f.to_proto()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let s = encode(&[cmd]);
        assert!(
            s.contains(r#""tf":{"op":"and","nodes":[{"key":"k","cmp":"eq","val":"v"}]}"#),
            "{s}"
        );
    }

    #[test]
    fn decodes_replies_and_pushes_with_zero_copy_payloads() {
        let frame = Bytes::from_static(
            b"{\"id\":1,\"connect\":{\"client\":\"c1\",\"version\":\"6.10.2\",\"ping\":25,\"pong\":true,\"data\":{\"a\":[1,2]}}}\n\
              {\"push\":{\"channel\":\"news\",\"pub\":{\"data\":{\"t\":\"hi\"},\"offset\":\"18446744073709551615\",\"tags\":{\"k\":\"v\"}}}}\n\
              \n\
              {}\n",
        );
        let replies = decode_frame(Protocol::Json, frame.clone(), MAX).unwrap();
        assert_eq!(replies.len(), 3);
        let c = replies[0].connect.as_ref().unwrap();
        assert_eq!((c.client.as_str(), c.ping, c.pong), ("c1", 25, true));
        assert_eq!(&c.data[..], br#"{"a":[1,2]}"#);
        // Zero copy: the payload points into the frame.
        let range = frame.as_ptr() as usize..frame.as_ptr() as usize + frame.len();
        assert!(range.contains(&(c.data.as_ptr() as usize)));
        let p = replies[1].push.as_ref().unwrap();
        let publication = p.r#pub.as_ref().unwrap();
        assert_eq!(publication.offset, u64::MAX);
        assert_eq!(publication.tags["k"], "v");
        assert_eq!(&publication.data[..], br#"{"t":"hi"}"#);
        assert_eq!(replies[2], proto::Reply::default());
    }

    #[test]
    fn unknown_fields_are_ignored_and_bad_shapes_rejected() {
        let ok = Bytes::from_static(br#"{"id":3,"future_field":{"x":1},"rpc":{"data":"str"}}"#);
        let r = decode_frame(Protocol::Json, ok, MAX).unwrap();
        assert_eq!(&r[0].rpc.as_ref().unwrap().data[..], br#""str""#);
        for bad in [&b"{"[..], b"[1]", br#"{"id":"x"}"#, b"<html>"] {
            assert!(
                decode_frame(Protocol::Json, Bytes::from_static(bad), MAX).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn emulation_request_carries_frame_as_json_string() {
        let body = encode_emulation_request(Protocol::Json, "n1", "s1", br#"{"id":1}"#).unwrap();
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"session":"s1","node":"n1","data":"{\"id\":1}"}"#
        );
    }
}

#[test]
fn protobuf_round_trip_and_zero_copy() {
    let reply = proto::Reply {
        push: Some(proto::Push {
            channel: "c".into(),
            r#pub: Some(proto::Publication {
                data: Bytes::from_static(b"\x00\x01binary"),
                offset: 7,
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut frame = Vec::new();
    reply.encode_length_delimited(&mut frame).unwrap();
    proto::Reply {
        id: 2,
        ..Default::default()
    }
    .encode_length_delimited(&mut frame)
    .unwrap();
    let frame = Bytes::from(frame);
    let got = decode_frame(Protocol::Protobuf, frame.clone(), MAX).unwrap();
    assert_eq!(got.len(), 2);
    let data = &got[0].push.as_ref().unwrap().r#pub.as_ref().unwrap().data;
    assert_eq!(&data[..], b"\x00\x01binary");
    let range = frame.as_ptr() as usize..frame.as_ptr() as usize + frame.len();
    assert!(range.contains(&(data.as_ptr() as usize)));

    let mut out = Vec::new();
    encode_commands(
        Protocol::Protobuf,
        &[sub_cmd(1, "a"), proto::Command::default()],
        &mut out,
    )
    .unwrap();
    let mut buf = Bytes::from(out);
    let first = proto::Command::decode_length_delimited(&mut buf).unwrap();
    assert_eq!(first, sub_cmd(1, "a"));
    let pong = proto::Command::decode_length_delimited(&mut buf).unwrap();
    assert_eq!(pong, proto::Command::default());
}

#[test]
fn frame_limits_and_truncation() {
    let mut frame = Vec::new();
    prost::encode_length_delimiter(100, &mut frame).unwrap();
    frame.extend_from_slice(&[0; 10]);
    assert_eq!(
        decode_frame(Protocol::Protobuf, Bytes::from(frame), MAX),
        Err(DecodeError::Truncated)
    );
    let mut frame = Vec::new();
    prost::encode_length_delimiter(5000, &mut frame).unwrap();
    assert!(matches!(
        decode_frame(Protocol::Protobuf, Bytes::from(frame), 1000),
        Err(DecodeError::TooLarge { .. })
    ));
}

fn stream_frames(protocol: Protocol) -> (Vec<u8>, Vec<proto::Reply>) {
    let replies: Vec<proto::Reply> = (1..=4u32)
        .map(|i| proto::Reply {
            id: i,
            rpc: Some(proto::RpcResult {
                data: Bytes::from(format!("{{\"n\":{i}}}")),
            }),
            ..Default::default()
        })
        .collect();
    let mut bytes = Vec::new();
    for r in &replies {
        match protocol {
            Protocol::Protobuf => r.encode_length_delimited(&mut bytes).unwrap(),
            #[cfg(feature = "json")]
            Protocol::Json => {
                bytes.extend_from_slice(
                    format!(
                        "{{\"id\":{},\"rpc\":{{\"data\":{}}}}}\n\n",
                        r.id,
                        std::str::from_utf8(&r.rpc.as_ref().unwrap().data).unwrap()
                    )
                    .as_bytes(),
                );
            }
        }
    }
    (bytes, replies)
}

fn protocols() -> Vec<Protocol> {
    vec![
        Protocol::Protobuf,
        #[cfg(feature = "json")]
        Protocol::Json,
    ]
}

#[test]
fn stream_decoder_reassembles_at_every_split_offset() {
    for protocol in protocols() {
        let (bytes, want) = stream_frames(protocol);
        for split in 0..=bytes.len() {
            let mut d = StreamDecoder::new(protocol, MAX);
            let mut got = Vec::new();
            d.push(&bytes[..split], &mut got).unwrap();
            d.push(&bytes[split..], &mut got).unwrap();
            d.finish().unwrap();
            assert_eq!(got, want, "{protocol:?} split at {split}");
        }
        // One byte at a time.
        let mut d = StreamDecoder::new(protocol, MAX);
        let mut got = Vec::new();
        for b in &bytes {
            d.push(std::slice::from_ref(b), &mut got).unwrap();
        }
        assert_eq!(got, want);
    }
}

#[test]
fn stream_decoder_reports_truncation_garbage_and_oversize() {
    for protocol in protocols() {
        let (bytes, _) = stream_frames(protocol);
        let mut d = StreamDecoder::new(protocol, MAX);
        let mut got = Vec::new();
        d.push(&bytes[..bytes.len() - 3], &mut got).unwrap();
        assert_eq!(d.finish(), Err(DecodeError::Truncated), "{protocol:?}");
    }
    // An HTML page read as Protobuf is either undecodable, or a length prefix
    // whose message never completes: an error by the end of the stream either way.
    let mut d = StreamDecoder::new(Protocol::Protobuf, MAX);
    let pushed = d.push(
        b"<!DOCTYPE html><html>captive portal</html>\n",
        &mut Vec::new(),
    );
    assert!(pushed.is_err() || d.finish().is_err());
    #[cfg(feature = "json")]
    {
        let mut d = StreamDecoder::new(Protocol::Json, MAX);
        assert!(d.push(b"<!DOCTYPE html>\n", &mut Vec::new()).is_err());
        let mut d = StreamDecoder::new(Protocol::Json, 16);
        assert!(matches!(
            d.push(&[b'x'; 17], &mut Vec::new()),
            Err(DecodeError::TooLarge { .. })
        ));
    }
}

#[test]
fn stream_decoder_keeps_replies_before_an_error() {
    let mut bytes = Vec::new();
    proto::Reply {
        id: 9,
        ..Default::default()
    }
    .encode_length_delimited(&mut bytes)
    .unwrap();
    bytes.extend_from_slice(&[0xff; 11]);
    let mut d = StreamDecoder::new(Protocol::Protobuf, MAX);
    let mut got = Vec::new();
    assert!(d.push(&bytes, &mut got).is_err());
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].id, 9);
}

#[test]
fn validate_payload_rules() {
    assert!(validate_payload(Protocol::Protobuf, "d", b"\xff\x00").is_ok());
    #[cfg(feature = "json")]
    {
        assert!(validate_payload(Protocol::Json, "d", b"").is_ok());
        assert!(validate_payload(Protocol::Json, "d", br#"{"a":1}"#).is_ok());
        assert!(validate_payload(Protocol::Json, "d", b"hello").is_err());
    }
}
