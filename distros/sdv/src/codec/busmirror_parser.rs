//! AUTOSAR Bus Mirroring v1 destination-frame parser.

use flow::codec::CodecError;

use crate::decoder::payload::FrameIdentity;

pub const HEADER_LEN: usize = 14;

/// A validated BusMirror item referencing bytes in its destination frame.
#[derive(Debug, Clone, Copy)]
pub struct BusMirrorFrameSlot {
    pub identity: FrameIdentity,
    pub payload_offset: u32,
    pub payload_len: u16,
}

/// Return the complete destination-frame length at `offset`.
///
/// An error means the outer boundary cannot be trusted, so callers must stop
/// scanning the remainder of the source payload.
pub fn packet_len(payload: &[u8], offset: usize) -> Result<usize, CodecError> {
    let remaining = payload.len().saturating_sub(offset);
    if remaining < HEADER_LEN {
        return Err(CodecError::Other(format!(
            "truncated BusMirror header at offset {offset}: expected {HEADER_LEN} bytes, remaining {remaining}"
        )));
    }
    let body_len = usize::from(u16::from_be_bytes([
        payload[offset + 12],
        payload[offset + 13],
    ]));
    let packet_len = HEADER_LEN.checked_add(body_len).ok_or_else(|| {
        CodecError::Other(format!(
            "BusMirror packet length overflow at offset {offset}: body length {body_len}"
        ))
    })?;
    if packet_len > remaining {
        return Err(CodecError::Other(format!(
            "truncated BusMirror packet at offset {offset}: expected {packet_len} bytes, remaining {remaining}"
        )));
    }
    Ok(packet_len)
}

/// Parse and validate one complete destination frame.
///
/// `slots` is cleared before parsing and remains empty on error, allowing a
/// merger to commit the packet atomically after this method succeeds.
pub fn parse_packet(packet: &[u8], slots: &mut Vec<BusMirrorFrameSlot>) -> Result<u64, CodecError> {
    slots.clear();
    let result = parse_packet_inner(packet, slots);
    if result.is_err() {
        slots.clear();
    }
    result
}

fn parse_packet_inner(
    packet: &[u8],
    slots: &mut Vec<BusMirrorFrameSlot>,
) -> Result<u64, CodecError> {
    if packet.len() < HEADER_LEN {
        return Err(CodecError::Other(format!(
            "truncated BusMirror header: expected {HEADER_LEN} bytes, got {}",
            packet.len()
        )));
    }
    if packet[0] != 1 {
        return Err(CodecError::Other(format!(
            "unsupported BusMirror protocol version {}; expected 1",
            packet[0]
        )));
    }

    let seconds = packet[2..8]
        .iter()
        .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
    let nanoseconds = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]);
    if nanoseconds >= 1_000_000_000 {
        return Err(CodecError::Other(format!(
            "invalid BusMirror header timestamp nanoseconds {nanoseconds}"
        )));
    }
    let timestamp = seconds
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(u64::from(nanoseconds / 1_000_000)))
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or_else(|| CodecError::Other("BusMirror header timestamp overflow".to_string()))?;

    let body_len = usize::from(u16::from_be_bytes([packet[12], packet[13]]));
    let expected_len = HEADER_LEN.checked_add(body_len).ok_or_else(|| {
        CodecError::Other("BusMirror destination-frame length overflow".to_string())
    })?;
    if packet.len() != expected_len {
        return Err(CodecError::Other(format!(
            "BusMirror destination-frame length mismatch: header declares {expected_len} bytes, got {}",
            packet.len()
        )));
    }

    let mut cursor = HEADER_LEN;
    while cursor < packet.len() {
        let item_offset = cursor;
        require(packet, cursor, 4, item_offset, "fixed fields")?;
        cursor += 2; // RelativeTimestamp is intentionally ignored in v1.
        let flags = packet[cursor];
        cursor += 1;
        let network_type = flags & 0x1f;
        let network_id = packet[cursor];
        cursor += 1;

        let network_state_available = flags & 0x80 != 0;
        let frame_id_available = flags & 0x40 != 0;
        let payload_available = flags & 0x20 != 0;

        if network_state_available {
            require(packet, cursor, 1, item_offset, "network state")?;
            cursor += 1;
        }

        let frame_id = if frame_id_available {
            let width = frame_id_width(network_type).ok_or_else(|| {
                CodecError::Other(format!(
                    "unsupported BusMirror network type {network_type} with FrameID at item offset {item_offset}"
                ))
            })?;
            require(packet, cursor, width, item_offset, "frame ID")?;
            let value = packet[cursor..cursor + width]
                .iter()
                .fold(0u32, |value, byte| (value << 8) | u32::from(*byte));
            cursor += width;
            Some(if network_type == 1 {
                crate::can_id::busmirror_can_id(value)
            } else {
                value
            })
        } else {
            None
        };

        let payload_range = if payload_available {
            require(packet, cursor, 1, item_offset, "payload length")?;
            let payload_len = usize::from(packet[cursor]);
            cursor += 1;
            require(packet, cursor, payload_len, item_offset, "payload")?;
            let start = cursor;
            cursor += payload_len;
            Some((start, payload_len))
        } else {
            None
        };

        if let (Some(frame_id), Some((payload_offset, payload_len))) = (frame_id, payload_range) {
            match network_type {
                1 | 2 => slots.push(BusMirrorFrameSlot {
                    identity: FrameIdentity::busmirror(network_type, network_id, frame_id),
                    payload_offset: u32::try_from(payload_offset).map_err(|_| {
                        CodecError::Other("BusMirror packet offset exceeds u32".to_string())
                    })?,
                    payload_len: u16::try_from(payload_len).map_err(|_| {
                        CodecError::Other("BusMirror item payload exceeds u16".to_string())
                    })?,
                }),
                3 => {} // FlexRay has a known boundary but no v1 signal decoder.
                _ => {}
            }
        }

        if cursor <= item_offset {
            return Err(CodecError::Other(format!(
                "BusMirror item cursor did not advance at offset {item_offset}"
            )));
        }
    }

    if cursor != packet.len() {
        return Err(CodecError::Other(format!(
            "BusMirror item parsing stopped at {cursor}, packet ends at {}",
            packet.len()
        )));
    }
    Ok(timestamp)
}

fn frame_id_width(network_type: u8) -> Option<usize> {
    match network_type {
        1 => Some(4),
        2 => Some(1),
        3 => Some(3),
        _ => None,
    }
}

fn require(
    packet: &[u8],
    cursor: usize,
    len: usize,
    item_offset: usize,
    field: &str,
) -> Result<(), CodecError> {
    if cursor.checked_add(len).is_none_or(|end| end > packet.len()) {
        return Err(CodecError::Other(format!(
            "truncated BusMirror {field} at item offset {item_offset}: need {len} bytes, remaining {}",
            packet.len().saturating_sub(cursor)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(seconds: u64, nanoseconds: u32, body: &[u8]) -> Vec<u8> {
        let mut packet = vec![1, 0];
        packet.extend_from_slice(&seconds.to_be_bytes()[2..]);
        packet.extend_from_slice(&nanoseconds.to_be_bytes());
        packet.extend_from_slice(&(body.len() as u16).to_be_bytes());
        packet.extend_from_slice(body);
        packet
    }

    #[test]
    fn parses_can_fd_and_lin_items_into_distinct_identities() {
        let mut body = Vec::new();
        body.extend_from_slice(&[0, 0, 0x61, 7]);
        body.extend_from_slice(&0xc000_0123u32.to_be_bytes());
        body.extend_from_slice(&[2, 0xaa, 0xbb]);
        body.extend_from_slice(&[0, 0, 0x62, 7, 0x23, 1, 0xcc]);
        let packet = packet(12, 345_000_000, &body);

        let mut slots = Vec::new();
        let timestamp = parse_packet(&packet, &mut slots).expect("parse packet");

        assert_eq!(timestamp, 12_345);
        assert_eq!(slots.len(), 2);
        assert_eq!(
            slots[0].identity,
            FrameIdentity::busmirror(1, 7, 0x8000_0123)
        );
        assert_eq!(slots[1].identity, FrameIdentity::busmirror(2, 7, 0x23));
        assert_eq!(
            &packet[slots[0].payload_offset as usize
                ..slots[0].payload_offset as usize + slots[0].payload_len as usize],
            &[0xaa, 0xbb]
        );
    }

    #[test]
    fn clears_can_fd_flag_and_keeps_standard_id() {
        let mut body = Vec::new();
        body.extend_from_slice(&[0, 0, 0x61, 1]);
        body.extend_from_slice(&0x4000_0100u32.to_be_bytes());
        body.extend_from_slice(&[1, 0xaa]);
        let packet = packet(1, 0, &body);

        let mut slots = Vec::new();
        parse_packet(&packet, &mut slots).expect("parse packet");
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].identity, FrameIdentity::busmirror(1, 1, 0x100));
    }

    #[test]
    fn malformed_item_clears_staged_slots() {
        let mut body = Vec::new();
        body.extend_from_slice(&[0, 0, 0x61, 1]);
        body.extend_from_slice(&0x123u32.to_be_bytes());
        body.extend_from_slice(&[1, 0xaa]);
        body.extend_from_slice(&[0, 0, 0x61, 1]);
        let packet = packet(1, 0, &body);
        let mut slots = Vec::new();

        assert!(parse_packet(&packet, &mut slots).is_err());
        assert!(slots.is_empty());
    }

    #[test]
    fn packet_len_rejects_a_truncated_outer_packet() {
        let packet = packet(1, 0, &[0, 0, 0x01, 1]);
        assert!(packet_len(&packet[..packet.len() - 1], 0).is_err());
    }
}
