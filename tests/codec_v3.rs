use bytes::{BufMut, BytesMut};
use lumenmq::codec::{MqttCodec, Packet, Publish, QoS, Subscribe, SubscribeTopic};
use tokio_util::codec::{Decoder, Encoder};

fn roundtrip(packet: Packet) -> Packet {
    let mut codec = MqttCodec::new(1024 * 1024);
    let mut buf = BytesMut::new();
    codec.encode(packet.clone(), &mut buf).expect("encode");
    // Framed 会把解码后的报文从 buf 中消费
    let decoded = codec.decode(&mut buf).expect("decode").expect("some");
    assert!(buf.is_empty(), "buffer should be fully consumed, {} bytes left", buf.len());
    decoded
}

#[test]
fn publish_qos0_roundtrip() {
    let p = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic: "sensor/temp".into(),
        packet_id: None,
        payload: b"23.5".to_vec(),
    });
    let decoded = roundtrip(p);
    match decoded {
        Packet::Publish(d) => {
            assert_eq!(d.topic, "sensor/temp");
            assert_eq!(d.qos, QoS::AtMostOnce);
            assert!(d.retain);
            assert_eq!(d.payload, b"23.5");
            assert!(d.packet_id.is_none());
        }
        other => panic!("expected Publish, got {other:?}"),
    }
}

#[test]
fn publish_qos1_roundtrip() {
    let p = Packet::Publish(Publish {
        dup: true,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "a/b".into(),
        packet_id: Some(1234),
        payload: vec![1, 2, 3, 4],
    });
    let decoded = roundtrip(p);
    match decoded {
        Packet::Publish(d) => {
            assert_eq!(d.qos, QoS::AtLeastOnce);
            assert!(d.dup);
            assert_eq!(d.packet_id, Some(1234));
            assert_eq!(d.payload, vec![1, 2, 3, 4]);
        }
        other => panic!("expected Publish, got {other:?}"),
    }
}

#[test]
fn subscribe_roundtrip() {
    let p = Packet::Subscribe(Subscribe {
        packet_id: 7,
        topics: vec![
            SubscribeTopic { topic_filter: "a/+".into(), qos: QoS::AtLeastOnce },
            SubscribeTopic { topic_filter: "#".into(), qos: QoS::AtMostOnce },
        ],
    });
    let decoded = roundtrip(p);
    match decoded {
        Packet::Subscribe(s) => {
            assert_eq!(s.packet_id, 7);
            assert_eq!(s.topics.len(), 2);
            assert_eq!(s.topics[0].topic_filter, "a/+");
            assert_eq!(s.topics[0].qos, QoS::AtLeastOnce);
            assert_eq!(s.topics[1].topic_filter, "#");
        }
        other => panic!("expected Subscribe, got {other:?}"),
    }
}

#[test]
fn empty_payload_packets_roundtrip() {
    assert!(matches!(roundtrip(Packet::Pingreq), Packet::Pingreq));
    assert!(matches!(roundtrip(Packet::Pingresp), Packet::Pingresp));
    assert!(matches!(roundtrip(Packet::Disconnect), Packet::Disconnect));
    assert!(matches!(roundtrip(Packet::Puback(42)), Packet::Puback(42)));
    assert!(matches!(roundtrip(Packet::Pubrec(42)), Packet::Pubrec(42)));
    assert!(matches!(roundtrip(Packet::Pubrel(42)), Packet::Pubrel(42)));
    assert!(matches!(roundtrip(Packet::Pubcomp(42)), Packet::Pubcomp(42)));
}

#[test]
fn incomplete_packet_returns_none() {
    let mut codec = MqttCodec::new(1024 * 1024);
    let mut buf = BytesMut::new();
    // 仅写入固定头声明 remaining=10，但不给足字节
    buf.put_u8(0x32); // PUBLISH qos1
    buf.put_u8(0x0A); // remaining length 10
    buf.put_slice(&[0, 3, b'a', b'/', b'b']); // 部分 topic
    assert!(codec.decode(&mut buf).expect("decode").is_none());
}
