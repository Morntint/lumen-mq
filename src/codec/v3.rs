use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::utils::CodecError;

use super::packet::*;

/// MQTT 3.1.1 编解码器，配合 tokio_util::codec::Framed 使用
#[derive(Debug, Clone)]
pub struct MqttCodec {
    max_packet_size: usize,
}

impl MqttCodec {
    pub fn new(max_packet_size: usize) -> Self {
        Self { max_packet_size }
    }
}

impl Default for MqttCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

impl Decoder for MqttCodec {
    type Item = Packet;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Packet>, Self::Error> {
        // 至少需要 2 字节（1 字节固定头 + 至少 1 字节剩余长度）
        if src.len() < 2 {
            return Ok(None);
        }
        // 解析剩余长度，确定整包长度
        let (remaining_len, header_len) = match decode_remaining_length(src)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let total = header_len + remaining_len;
        if total > self.max_packet_size {
            src.advance(total); // 丢弃越界包
            return Err(CodecError::MalformedBody(format!(
                "packet length {total} exceeds max {}",
                self.max_packet_size
            )));
        }
        if src.len() < total {
            // 预分配容量，避免多次扩容
            src.reserve(total - src.len());
            return Ok(None);
        }
        // 取出完整包
        let frame = src.split_to(total);
        // 固定头首字节
        let first = frame[0];
        // 载荷从固定头之后开始（首字节 + 剩余长度字节）
        let payload = &frame[header_len..];
        let packet = decode_packet(first, payload)?;
        Ok(Some(packet))
    }
}

impl Encoder<Packet> for MqttCodec {
    type Error = CodecError;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut body = BytesMut::new();
        let (ptype, flags) = encode_packet_body(&item, &mut body)?;
        let body_len = body.len();

        // 剩余长度上限校验
        if body_len > 0x0FFF_FFFF {
            return Err(CodecError::InvalidRemainingLength(body_len));
        }

        // 固定头
        let first_byte = (ptype as u8) << 4 | (flags & 0x0F);
        dst.put_u8(first_byte);
        encode_remaining_length(body_len, dst);
        dst.put_slice(&body);
        Ok(())
    }
}

// ---------- 固定头/剩余长度 ----------

/// 解析剩余长度，返回 (remaining_len, header_len_including_first_byte)
/// 若数据不足返回 Ok(None)
fn decode_remaining_length(src: &[u8]) -> Result<Option<(usize, usize)>, CodecError> {
    let mut multiplier: usize = 1;
    let mut value: usize = 0;
    let mut idx = 1;
    loop {
        if idx >= src.len() {
            return Ok(None); // 需要更多字节
        }
        let byte = src[idx];
        value += ((byte & 0x7F) as usize) * multiplier;
        idx += 1;
        if (byte & 0x80) == 0 {
            return Ok(Some((value, idx)));
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(CodecError::InvalidRemainingLength(value));
        }
    }
}

fn encode_remaining_length(mut value: usize, dst: &mut BytesMut) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        dst.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

// ---------- 报文体解析 ----------

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
    fn read_u8(&mut self) -> Result<u8, CodecError> {
        if self.pos >= self.buf.len() {
            return Err(CodecError::MalformedBody("unexpected eof reading u8".into()));
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }
    fn read_u16(&mut self) -> Result<u16, CodecError> {
        if self.remaining() < 2 {
            return Err(CodecError::MalformedBody("unexpected eof reading u16".into()));
        }
        let v = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }
    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.remaining() < n {
            return Err(CodecError::MalformedBody(format!("unexpected eof reading {n} bytes")));
        }
        let v = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(v)
    }
    fn read_string(&mut self) -> Result<String, CodecError> {
        let len = self.read_u16()? as usize;
        let raw = self.read_bytes(len)?;
        std::str::from_utf8(raw)
            .map(|s| s.to_string())
            .map_err(|_| CodecError::MalformedUtf8)
    }
    /// 读取二进制数据（带 2 字节长度前缀）
    fn read_binary(&mut self) -> Result<Vec<u8>, CodecError> {
        let len = self.read_u16()? as usize;
        Ok(self.read_bytes(len)?.to_vec())
    }
    /// 读取 MQTT 变长字节整数（VBI），用于 5.0 属性长度 / 属性标识
    fn read_vbi(&mut self) -> Result<u32, CodecError> {
        let mut multiplier: u32 = 1;
        let mut value: u32 = 0;
        loop {
            let byte = self.read_u8()?;
            value += ((byte & 0x7F) as u32) * multiplier;
            if (byte & 0x80) == 0 {
                return Ok(value);
            }
            multiplier *= 128;
            if multiplier > 128 * 128 * 128 {
                return Err(CodecError::MalformedBody("vbi overflow".into()));
            }
        }
    }
    /// 跳过 n 字节
    fn skip(&mut self, n: usize) -> Result<(), CodecError> {
        self.read_bytes(n)?;
        Ok(())
    }
}

/// MQTT 5.0 属性标识常量（仅列出轻量解析关注的项）
mod prop_id {
    /// 会话过期间隔（uint32，秒）
    pub const SESSION_EXPIRY_INTERVAL: u32 = 0x11;
}

/// 解析 MQTT 5.0 CONNECT 属性段，仅提取 session_expiry_interval，其余跳过。
/// `props_len` 为属性段总字节数。
fn parse_connect_properties(r: &mut Reader, props_len: usize) -> Result<ConnectProperties, CodecError> {
    let mut props = ConnectProperties::default();
    let end = r.pos + props_len;
    if props_len == 0 {
        return Ok(props);
    }
    while r.pos < end {
        let id = r.read_vbi()?;
        match id {
            prop_id::SESSION_EXPIRY_INTERVAL => {
                if r.remaining() < 4 {
                    return Err(CodecError::MalformedBody("session expiry interval truncated".into()));
                }
                let raw = r.read_bytes(4)?;
                props.session_expiry_interval = Some(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]));
            }
            // 未识别的属性：按类型跳过。MQTT 5.0 属性值类型映射：
            // 字符串/二进制：2 字节长度前缀 + 数据；uint8/u8；uint16/u16；uint32/u32
            other => skip_property_value(r, other)?,
        }
    }
    Ok(props)
}

/// 按属性标识跳过其值（轻量实现：覆盖 MQTT 5.0 全部已注册属性标识的值类型）
fn skip_property_value(r: &mut Reader, id: u32) -> Result<(), CodecError> {
    /// 字符串/二进制类型：2 字节长度前缀 + 数据
    fn skip_string_or_binary(r: &mut Reader) -> Result<(), CodecError> {
        let len = r.read_u16()? as usize;
        r.skip(len)
    }
    /// 用户属性：两个长度前缀字符串
    fn skip_string_pair(r: &mut Reader) -> Result<(), CodecError> {
        skip_string_or_binary(r)?;
        skip_string_or_binary(r)
    }
    match id {
        // byte (u8) 类型
        0x01 | 0x17 | 0x19 | 0x22 | 0x23 | 0x26 | 0x27 | 0x28 => { r.read_u8()?; Ok(()) }
        // uint16 类型
        0x13 | 0x1E | 0x1F | 0x21 => { r.read_u16()?; Ok(()) }
        // uint32 类型
        0x02 | 0x11 | 0x18 | 0x25 => { r.read_bytes(4)?; Ok(()) }
        // VBI 类型
        0x0B => { r.read_vbi()?; Ok(()) }
        // UTF-8 字符串类型
        0x03 | 0x08 | 0x0C | 0x12 | 0x15 | 0x1A | 0x1C | 0x1D => skip_string_or_binary(r),
        // 二进制类型
        0x09 | 0x16 => skip_string_or_binary(r),
        // 字符串对（User Property）
        0x24 => skip_string_pair(r),
        // 默认：按字符串/二进制尝试跳过；无法识别时报错避免静默错位
        _ => Err(CodecError::MalformedBody(format!("unknown mqtt5 property id 0x{id:02x}"))),
    }
}

fn decode_packet(first: u8, payload: &[u8]) -> Result<Packet, CodecError> {
    let ptype = PacketType::from_u8(first >> 4)?;
    let flags = first & 0x0F;
    let mut r = Reader::new(payload);

    let packet = match ptype {
        PacketType::Connect => decode_connect(&mut r)?,
        PacketType::Connack => decode_connack(&mut r)?,
        PacketType::Publish => decode_publish(&mut r, flags)?,
        PacketType::Puback => Packet::Puback(r.read_u16()?),
        PacketType::Pubrec => Packet::Pubrec(r.read_u16()?),
        PacketType::Pubrel => {
            validate_flags(ptype, flags)?;
            Packet::Pubrel(r.read_u16()?)
        }
        PacketType::Pubcomp => Packet::Pubcomp(r.read_u16()?),
        PacketType::Subscribe => {
            validate_flags(ptype, flags)?;
            decode_subscribe(&mut r)?
        }
        PacketType::Suback => decode_suback(&mut r)?,
        PacketType::Unsubscribe => {
            validate_flags(ptype, flags)?;
            decode_unsubscribe(&mut r)?
        }
        PacketType::Unsuback => Packet::Unsuback(r.read_u16()?),
        PacketType::Pingreq => {
            validate_flags(ptype, flags)?;
            Packet::Pingreq
        }
        PacketType::Pingresp => {
            validate_flags(ptype, flags)?;
            Packet::Pingresp
        }
        PacketType::Disconnect => {
            validate_flags(ptype, flags)?;
            Packet::Disconnect
        }
    };
    Ok(packet)
}

fn validate_flags(ptype: PacketType, flags: u8) -> Result<(), CodecError> {
    let expected = match ptype {
        PacketType::Pubrel | PacketType::Subscribe | PacketType::Unsubscribe => 0b0010,
        PacketType::Connect
        | PacketType::Connack
        | PacketType::Puback
        | PacketType::Pubrec
        | PacketType::Pubcomp
        | PacketType::Suback
        | PacketType::Unsuback
        | PacketType::Pingreq
        | PacketType::Pingresp
        | PacketType::Disconnect => 0b0000,
        PacketType::Publish => return Ok(()), // Publish flags 动态
    };
    if flags != expected {
        return Err(CodecError::InvalidFlags(ptype as u8, flags));
    }
    Ok(())
}

fn decode_connect(r: &mut Reader) -> Result<Packet, CodecError> {
    // 变长头
    let protocol_name = r.read_string()?;
    if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
        return Err(CodecError::InvalidProtocol(format!(
            "unknown protocol name '{protocol_name}'"
        )));
    }
    let protocol_level = r.read_u8()?;
    if protocol_level != MQTT_3_1_1_LEVEL && protocol_level != MQTT_3_1_LEVEL && protocol_level != MQTT_5_LEVEL {
        return Err(CodecError::UnsupportedVersion(protocol_level));
    }
    let connect_flags_byte = r.read_u8()?;
    let flags = ConnectFlags::from_bits_truncate(connect_flags_byte);
    let keep_alive = r.read_u16()?;

    // MQTT 5.0：CONNECT 在 keep_alive 后、载荷前有属性段
    let properties = if protocol_level == MQTT_5_LEVEL {
        let props_len = r.read_vbi()? as usize;
        if r.remaining() < props_len {
            return Err(CodecError::MalformedBody("connect properties truncated".into()));
        }
        Some(parse_connect_properties(r, props_len)?)
    } else {
        None
    };

    // 载荷顺序（5.0）：ClientID, [WillProperties, WillTopic, WillPayload], [Username, Password]
    // 载荷顺序（3.1.1）：ClientID, [WillTopic, WillMessage], [Username, Password]
    let client_id = r.read_string()?;
    let clean_session = flags.contains(ConnectFlags::CLEAN_SESSION);

    let will = if flags.contains(ConnectFlags::WILL_FLAG) {
        let will_qos = match (connect_flags_byte & ConnectFlags::WILL_QOS_MASK.bits()) >> 3 {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            other => return Err(CodecError::MalformedBody(format!("invalid will qos {other}"))),
        };
        // MQTT 5.0：遗嘱在 topic 前有 Will Properties 段（轻量实现：跳过）
        if protocol_level == MQTT_5_LEVEL {
            let will_props_len = r.read_vbi()? as usize;
            if r.remaining() < will_props_len {
                return Err(CodecError::MalformedBody("will properties truncated".into()));
            }
            r.skip(will_props_len)?;
        }
        let topic = r.read_string()?;
        let message = r.read_binary()?;
        Some(LastWill {
            topic,
            message,
            qos: will_qos,
            retain: flags.contains(ConnectFlags::WILL_RETAIN),
        })
    } else {
        None
    };

    let username = if flags.contains(ConnectFlags::USERNAME) {
        Some(r.read_string()?)
    } else {
        None
    };
    let password = if flags.contains(ConnectFlags::PASSWORD) {
        Some(r.read_binary()?)
    } else {
        None
    };

    Ok(Packet::Connect(Connect {
        protocol_level,
        keep_alive,
        client_id,
        clean_session,
        will,
        username,
        password,
        properties,
    }))
}

fn decode_connack(r: &mut Reader) -> Result<Packet, CodecError> {
    let flags_byte = r.read_u8()?;
    let session_present = (flags_byte & 0x01) != 0;
    let return_code = r.read_u8()?;
    // MQTT 5.0 CONNACK 在 return_code 后有属性段；轻量实现：跳过属性
    // 协议版本推断：有剩余字节（属性段）则 level=5，否则 level=4（3.1.1）
    let protocol_level = if r.remaining() > 0 {
        let props_len = r.read_vbi()? as usize;
        if r.remaining() < props_len {
            return Err(CodecError::MalformedBody("connack properties truncated".into()));
        }
        r.skip(props_len)?;
        MQTT_5_LEVEL
    } else {
        MQTT_3_1_1_LEVEL
    };
    Ok(Packet::Connack(Connack {
        session_present,
        return_code,
        protocol_level,
    }))
}

fn decode_publish(r: &mut Reader, flags: u8) -> Result<Packet, CodecError> {
    let dup = (flags & 0b1000) != 0;
    let qos = QoS::from_u8((flags & 0b0110) >> 1)?;
    let retain = (flags & 0b0001) != 0;
    let topic = r.read_string()?;
    let packet_id = if qos != QoS::AtMostOnce {
        Some(r.read_u16()?)
    } else {
        None
    };
    let payload = r.read_bytes(r.remaining())?.to_vec();
    Ok(Packet::Publish(Publish {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        payload,
    }))
}

fn decode_subscribe(r: &mut Reader) -> Result<Packet, CodecError> {
    let packet_id = r.read_u16()?;
    let mut topics = Vec::new();
    while r.remaining() > 0 {
        let topic_filter = r.read_string()?;
        let qos_byte = r.read_u8()?;
        let qos = QoS::from_u8(qos_byte)?;
        topics.push(SubscribeTopic { topic_filter, qos });
    }
    if topics.is_empty() {
        return Err(CodecError::MalformedBody("subscribe with no topic".into()));
    }
    Ok(Packet::Subscribe(Subscribe { packet_id, topics }))
}

fn decode_suback(r: &mut Reader) -> Result<Packet, CodecError> {
    let packet_id = r.read_u16()?;
    let mut return_codes = Vec::new();
    while r.remaining() > 0 {
        return_codes.push(r.read_u8()?);
    }
    Ok(Packet::Suback(Suback {
        packet_id,
        return_codes,
    }))
}

fn decode_unsubscribe(r: &mut Reader) -> Result<Packet, CodecError> {
    let packet_id = r.read_u16()?;
    let mut topic_filters = Vec::new();
    while r.remaining() > 0 {
        topic_filters.push(r.read_string()?);
    }
    if topic_filters.is_empty() {
        return Err(CodecError::MalformedBody("unsubscribe with no topic".into()));
    }
    Ok(Packet::Unsubscribe(Unsubscribe {
        packet_id,
        topic_filters,
    }))
}

// ---------- 报文体编码 ----------

fn encode_packet_body(p: &Packet, out: &mut BytesMut) -> Result<(PacketType, u8), CodecError> {
    match p {
        Packet::Connect(c) => {
            encode_connect(c, out);
            Ok((PacketType::Connect, 0))
        }
        Packet::Connack(c) => {
            out.put_u8(if c.session_present { 0x01 } else { 0x00 });
            out.put_u8(c.return_code);
            // MQTT 5.0 CONNACK 需要属性段；轻量实现：写空属性长度（0）
            if c.protocol_level == MQTT_5_LEVEL {
                encode_remaining_length(0, out);
            }
            Ok((PacketType::Connack, 0))
        }
        Packet::Publish(pub_) => {
            let flags = encode_publish(pub_, out);
            Ok((PacketType::Publish, flags))
        }
        Packet::Puback(id) => {
            out.put_u16(*id);
            Ok((PacketType::Puback, 0))
        }
        Packet::Pubrec(id) => {
            out.put_u16(*id);
            Ok((PacketType::Pubrec, 0))
        }
        Packet::Pubrel(id) => {
            out.put_u16(*id);
            Ok((PacketType::Pubrel, 0b0010))
        }
        Packet::Pubcomp(id) => {
            out.put_u16(*id);
            Ok((PacketType::Pubcomp, 0))
        }
        Packet::Subscribe(s) => {
            encode_subscribe(s, out);
            Ok((PacketType::Subscribe, 0b0010))
        }
        Packet::Suback(s) => {
            out.put_u16(s.packet_id);
            for &c in &s.return_codes {
                out.put_u8(c);
            }
            Ok((PacketType::Suback, 0))
        }
        Packet::Unsubscribe(u) => {
            encode_unsubscribe(u, out);
            Ok((PacketType::Unsubscribe, 0b0010))
        }
        Packet::Unsuback(id) => {
            out.put_u16(*id);
            Ok((PacketType::Unsuback, 0))
        }
        Packet::Pingreq => Ok((PacketType::Pingreq, 0)),
        Packet::Pingresp => Ok((PacketType::Pingresp, 0)),
        Packet::Disconnect => Ok((PacketType::Disconnect, 0)),
    }
}

fn put_string(out: &mut BytesMut, s: &str) {
    out.put_u16(s.len() as u16);
    out.put_slice(s.as_bytes());
}

fn put_binary(out: &mut BytesMut, b: &[u8]) {
    out.put_u16(b.len() as u16);
    out.put_slice(b);
}

fn encode_connect(c: &Connect, out: &mut BytesMut) {
    put_string(out, "MQTT");
    out.put_u8(c.protocol_level);

    let mut flags = ConnectFlags::empty();
    if c.clean_session {
        flags |= ConnectFlags::CLEAN_SESSION;
    }
    if let Some(will) = &c.will {
        flags |= ConnectFlags::WILL_FLAG;
        flags |= match will.qos {
            QoS::AtMostOnce => ConnectFlags::WILL_QOS_0,
            QoS::AtLeastOnce => ConnectFlags::WILL_QOS_1,
            QoS::ExactlyOnce => ConnectFlags::WILL_QOS_2,
        };
        if will.retain {
            flags |= ConnectFlags::WILL_RETAIN;
        }
    }
    if c.username.is_some() {
        flags |= ConnectFlags::USERNAME;
    }
    if c.password.is_some() {
        flags |= ConnectFlags::PASSWORD;
    }
    out.put_u8(flags.bits());
    out.put_u16(c.keep_alive);

    // MQTT 5.0：CONNECT 在 keep_alive 后写属性段
    // 轻量实现：仅写 Session Expiry Interval（若存在），其余属性不编码
    if c.protocol_level == MQTT_5_LEVEL {
        let mut props_body = BytesMut::new();
        if let Some(p) = &c.properties {
            if let Some(sei) = p.session_expiry_interval {
                props_body.put_u8(0x11); // Session Expiry Interval 标识
                props_body.put_u32(sei);
            }
        }
        encode_remaining_length(props_body.len(), out);
        out.put_slice(&props_body);
    }

    put_string(out, &c.client_id);
    if let Some(will) = &c.will {
        // MQTT 5.0：遗嘱在 topic 前写 Will Properties 段（轻量：空属性）
        if c.protocol_level == MQTT_5_LEVEL {
            encode_remaining_length(0, out);
        }
        put_string(out, &will.topic);
        put_binary(out, &will.message);
    }
    if let Some(u) = &c.username {
        put_string(out, u);
    }
    if let Some(p) = &c.password {
        put_binary(out, p);
    }
}

fn encode_publish(p: &Publish, out: &mut BytesMut) -> u8 {
    put_string(out, &p.topic);
    if let Some(id) = p.packet_id {
        out.put_u16(id);
    }
    out.put_slice(&p.payload);

    let mut flags: u8 = 0;
    if p.dup {
        flags |= 0b1000;
    }
    flags |= (p.qos as u8 & 0b11) << 1;
    if p.retain {
        flags |= 0b0001;
    }
    flags
}

fn encode_subscribe(s: &Subscribe, out: &mut BytesMut) {
    out.put_u16(s.packet_id);
    for t in &s.topics {
        put_string(out, &t.topic_filter);
        out.put_u8(t.qos.as_u8());
    }
}

fn encode_unsubscribe(u: &Unsubscribe, out: &mut BytesMut) {
    out.put_u16(u.packet_id);
    for f in &u.topic_filters {
        put_string(out, f);
    }
}

// ---------- 便捷构造 ----------

impl Packet {
    pub fn connack_accepted(session_present: bool) -> Self {
        Packet::Connack(Connack::accepted(session_present))
    }
    pub fn connack_error(code: u8) -> Self {
        Packet::Connack(Connack {
            session_present: false,
            return_code: code,
            protocol_level: MQTT_3_1_1_LEVEL,
        })
    }
}
