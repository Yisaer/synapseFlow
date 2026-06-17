//! GBF packet parser for schema-driven binary packet parsing.
//!
//! This module provides shared GBF packet parsing logic that can be reused
//! by both decoders (for decoding to records) and mergers (for repacking).

use crate::schema::gbf::{Field, GbfSchema};
use flow::codec::CodecError;
use std::collections::HashMap;

/// A parsed frame from a GBF packet.
#[derive(Debug, Clone)]
pub struct GbfFrame {
    pub can_id: u32,
    pub payload: Vec<u8>,
}

/// Field name mappings extracted from schema for dynamic encoding/decoding.
#[derive(Debug, Clone)]
pub struct SchemaFieldMap {
    /// Timestamp field name in root structure (e.g., "ts")
    pub timestamp_field: String,
    /// Sequence field name in root structure (e.g., "frames")
    pub sequence_field: String,
    /// Item type name for the sequence (e.g., "can_frame")
    pub sequence_item_type: String,
    /// CAN ID field name in frame items (e.g., "can_id")  
    pub frame_id_field: String,
    /// Payload field name in frame items (e.g., "payload")
    pub frame_payload_field: String,
}

impl SchemaFieldMap {
    /// Extract field name mappings from a GBF schema.
    ///
    /// The timestamp field must be named "ts" or "timestamp".
    pub fn from_schema(schema: &GbfSchema) -> Result<Self, CodecError> {
        let root = &schema.structure;

        // Find timestamp field - must be named "ts" or "timestamp"
        let timestamp_field = root
            .fields
            .iter()
            .find(|f| f.name == "ts" || f.name == "timestamp")
            .map(|f| f.name.clone())
            .ok_or_else(|| {
                CodecError::Other(
                    "No timestamp field found in root structure. Field must be named 'ts' or 'timestamp'."
                        .to_string(),
                )
            })?;

        // Find sequence field
        let sequence_field_def = root
            .fields
            .iter()
            .find(|f| f.field_type == "sequence")
            .ok_or_else(|| {
                CodecError::Other("No sequence field found in root structure".to_string())
            })?;

        let sequence_field = sequence_field_def.name.clone();

        // Validate: no bytes field may appear before the sequence length field.
        // split_packets() uses fixed-width arithmetic to locate the length field;
        // a bytes field with a variable length would make that offset incalculable.
        if let Some(ref_name) = sequence_field_def.length_ref.as_ref() {
            for f in root.fields.iter().take_while(|f| &f.name != ref_name) {
                if f.field_type == "bytes" {
                    return Err(CodecError::Other(format!(
                        "bytes field '{}' appears before the sequence length field '{}' \
                         in the root structure; packet splitting requires all header \
                         fields preceding the length field to be fixed-width",
                        f.name, ref_name
                    )));
                }
            }
        }

        let sequence_item = sequence_field_def.structure.as_ref().ok_or_else(|| {
            CodecError::Other("Sequence field missing structure definition".to_string())
        })?;

        let sequence_item_type = sequence_item.type_name.clone();

        // Get the frame type definition
        let frame_fields = &sequence_item.fields;

        // Exactly one bytes field must carry 'format', and it must have 'format.id_ref'.
        // Deriving both frame_payload_field and frame_id_field from the same field ensures
        // they can never disagree.
        let payload_field = {
            let mut candidates = frame_fields.iter().filter(|f| f.format.is_some());
            let first = candidates.next().ok_or_else(|| {
                CodecError::Other(
                    "No payload field (bytes field with 'format') found in frame type".to_string(),
                )
            })?;
            if candidates.next().is_some() {
                return Err(CodecError::Other(
                    "Frame struct has more than one field with 'format'; \
                     exactly one bytes payload field is allowed"
                        .to_string(),
                ));
            }
            if first.field_type != "bytes" {
                return Err(CodecError::Other(format!(
                    "Field '{}' has 'format' but is not a bytes field (type: '{}'). \
                     'format' is only valid on bytes fields.",
                    first.name, first.field_type
                )));
            }
            first
        };
        let frame_payload_field = payload_field.name.clone();
        let frame_id_field = payload_field
            .format
            .as_ref()
            .and_then(|fmt| fmt.id_ref.as_ref())
            .ok_or_else(|| {
                CodecError::Other(format!(
                    "Payload field '{}' is missing 'format.id_ref'; \
                     it must point to the ID field in the frame struct.",
                    frame_payload_field
                ))
            })?
            .clone();

        Ok(Self {
            timestamp_field,
            sequence_field,
            sequence_item_type,
            frame_id_field,
            frame_payload_field,
        })
    }
}

/// Internal optimized representation of a field.
#[derive(Debug, Clone)]
pub(crate) struct OptField {
    pub _name: String,
    pub kind: OptFieldType,
    pub index: usize,
    /// If true, this field's value is needed solely for calculating timestamp (ts).
    pub is_ts: bool,
    /// If set, the value must be stored because it is referenced by a length_ref or id_ref
    pub store_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) enum OptFieldType {
    U8 {
        const_val: Option<u64>,
        mask: Option<u64>,
        shift: Option<u32>,
    },
    U16Be {
        id_ref: bool,
    },
    U16Le {
        id_ref: bool,
    },
    U32Be {
        id_ref: bool,
    },
    U32Le {
        id_ref: bool,
    },
    U64Be,
    U64Le,
    Sequence {
        length_ref_index: usize,
        /// Pre-compiled item struct for this specific sequence field.
        item_def: Option<Box<OptTypeDef>>,
    },
    Bytes {
        length_ref_index: usize,
        format_id_ref_index: Option<usize>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct OptTypeDef {
    pub fields: Vec<OptField>,
    /// Number of storage slots needed for this type's context
    pub storage_size: usize,
}

/// GBF parser that converts binary packets into structured frames based on JSON schema.
pub struct GbfParser {
    packet_def: OptTypeDef,
    field_map: SchemaFieldMap,
}

impl GbfParser {
    /// Create a new GBF parser from a schema.
    pub fn new(gbf_schema: GbfSchema) -> Result<Self, CodecError> {
        let field_map = SchemaFieldMap::from_schema(&gbf_schema)?;

        let packet_def = Self::compile_type_def_from_fields(&gbf_schema.structure.fields)?;

        Ok(Self {
            packet_def,
            field_map,
        })
    }

    /// Get the schema field name mappings.
    pub fn field_map(&self) -> &SchemaFieldMap {
        &self.field_map
    }

    fn compile_type_def_from_fields(fields_def: &[Field]) -> Result<OptTypeDef, CodecError> {
        let mut fields = Vec::with_capacity(fields_def.len());
        let mut name_to_index = HashMap::new();

        // First pass: assign storage indices to all fields that might be referenced
        for (i, field) in fields_def.iter().enumerate() {
            name_to_index.insert(field.name.clone(), i);
        }

        for (i, field) in fields_def.iter().enumerate() {
            let kind = match field.field_type.as_str() {
                "u8" => OptFieldType::U8 {
                    const_val: field.const_value,
                    mask: field.read_mask,
                    shift: field.read_shift,
                },
                "u16be" => {
                    let is_id_ref = fields_def.iter().any(|f| {
                        f.format.as_ref().and_then(|fmt| fmt.id_ref.as_ref()) == Some(&field.name)
                    });
                    OptFieldType::U16Be { id_ref: is_id_ref }
                }
                "u16le" => {
                    let is_id_ref = fields_def.iter().any(|f| {
                        f.format.as_ref().and_then(|fmt| fmt.id_ref.as_ref()) == Some(&field.name)
                    });
                    OptFieldType::U16Le { id_ref: is_id_ref }
                }
                "u32be" => {
                    let is_id_ref = fields_def.iter().any(|f| {
                        f.format.as_ref().and_then(|fmt| fmt.id_ref.as_ref()) == Some(&field.name)
                    });
                    OptFieldType::U32Be { id_ref: is_id_ref }
                }
                "u32le" => {
                    let is_id_ref = fields_def.iter().any(|f| {
                        f.format.as_ref().and_then(|fmt| fmt.id_ref.as_ref()) == Some(&field.name)
                    });
                    OptFieldType::U32Le { id_ref: is_id_ref }
                }
                "u64be" => OptFieldType::U64Be,
                "u64le" => OptFieldType::U64Le,
                "sequence" => {
                    let ref_name = field.length_ref.as_ref().ok_or_else(|| {
                        CodecError::Other(format!(
                            "sequence field '{}' is missing 'length_ref'",
                            field.name
                        ))
                    })?;
                    let ref_idx = *name_to_index.get(ref_name).ok_or_else(|| {
                        CodecError::Other(format!(
                            "sequence field '{}' has unknown length_ref '{}'",
                            field.name, ref_name
                        ))
                    })?;
                    if ref_idx >= i {
                        return Err(CodecError::Other(format!(
                            "sequence field '{}' has length_ref '{}' that must refer to an earlier field",
                            field.name, ref_name
                        )));
                    }
                    let item_def = field
                        .structure
                        .as_ref()
                        .ok_or_else(|| {
                            CodecError::Other(format!(
                                "sequence field '{}' is missing 'structure' definition",
                                field.name
                            ))
                        })
                        .and_then(|s| {
                            if let Some(nested) =
                                s.fields.iter().find(|f| f.field_type == "sequence")
                            {
                                return Err(CodecError::Other(format!(
                                    "sequence field '{}' contains unsupported nested \
                                     sequence field '{}'",
                                    field.name, nested.name
                                )));
                            }
                            Self::compile_type_def_from_fields(&s.fields).map(Box::new)
                        })?;
                    if let Some(unit) = field.length_unit.as_deref()
                        && unit != "bytes"
                    {
                        return Err(CodecError::Other(format!(
                            "sequence field '{}' has unsupported length_unit '{}'; \
                             only \"bytes\" is supported",
                            field.name, unit
                        )));
                    }
                    OptFieldType::Sequence {
                        length_ref_index: ref_idx,
                        item_def: Some(item_def),
                    }
                }
                "bytes" => {
                    let ref_name = field.length_ref.as_ref().ok_or_else(|| {
                        CodecError::Other(format!(
                            "bytes field '{}' is missing 'length_ref'",
                            field.name
                        ))
                    })?;
                    let ref_idx = *name_to_index.get(ref_name).ok_or_else(|| {
                        CodecError::Other(format!(
                            "bytes field '{}' has unknown length_ref '{}'",
                            field.name, ref_name
                        ))
                    })?;
                    if ref_idx >= i {
                        return Err(CodecError::Other(format!(
                            "bytes field '{}' has length_ref '{}' that must refer to an earlier field",
                            field.name, ref_name
                        )));
                    }
                    let id_ref_idx = if let Some(fmt) = field.format.as_ref() {
                        if let Some(name) = fmt.id_ref.as_ref() {
                            let idx = *name_to_index.get(name).ok_or_else(|| {
                                CodecError::Other(format!(
                                    "bytes field '{}' has unknown id_ref '{}'",
                                    field.name, name
                                ))
                            })?;
                            const CAN_ID_INTEGER_TYPES: &[&str] =
                                &["u8", "u16be", "u16le", "u32be", "u32le"];
                            let target_type = fields_def[idx].field_type.as_str();
                            if !CAN_ID_INTEGER_TYPES.contains(&target_type) {
                                return Err(CodecError::Other(format!(
                                    "bytes field '{}' has id_ref '{}' pointing to \
                                     unsupported field type '{}'; id_ref must refer \
                                     to a CAN-ID-compatible integer field (u8/u16*/u32*)",
                                    field.name, name, target_type
                                )));
                            }
                            Some(idx)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    OptFieldType::Bytes {
                        length_ref_index: ref_idx,
                        format_id_ref_index: id_ref_idx,
                    }
                }
                _ => {
                    return Err(CodecError::Other(format!(
                        "field '{}' has unsupported type '{}'",
                        field.name, field.field_type
                    )));
                }
            };

            fields.push(OptField {
                _name: field.name.clone(),
                kind,
                index: i,
                is_ts: field.name == "ts" || field.name == "timestamp",
                store_index: Some(i), // Store everything for now
            });
        }

        Ok(OptTypeDef {
            fields,
            storage_size: fields_def.len(),
        })
    }

    /// Split a payload into individual packets based on the schema.
    pub fn split_packets<'a>(&self, payload: &'a [u8]) -> Vec<&'a [u8]> {
        let mut packets = Vec::new();
        let mut cursor = 0;

        // Calculate packet header size (all fixed fields before the sequence)
        let header_size = self.calculate_header_size(&self.packet_def);

        while cursor + header_size <= payload.len() {
            if let Some((len_offset, len_size)) = self.find_length_field_offset(&self.packet_def) {
                let len_bytes = &payload[cursor + len_offset..cursor + len_offset + len_size];
                let body_len = self.parse_uint(len_bytes, len_size) as usize;
                let packet_size = header_size + body_len;

                if cursor + packet_size > payload.len() {
                    break;
                }

                packets.push(&payload[cursor..cursor + packet_size]);
                cursor += packet_size;
            } else {
                packets.push(&payload[cursor..]);
                break;
            }
        }

        packets
    }

    /// Parse a single packet into timestamp and frames based on the schema.
    pub fn parse_packet(&self, packet: &[u8]) -> Result<(u64, Vec<GbfFrame>), CodecError> {
        let mut frames = Vec::new();
        let timestamp = self.parse_packet_with(packet, &mut |can_id, payload| {
            frames.push(GbfFrame {
                can_id,
                payload: payload.to_vec(),
            });
        })?;
        Ok((timestamp, frames))
    }

    /// Parse a packet, invoking `sink(can_id, payload)` for each frame without
    /// allocating a per-frame payload buffer. Returns the packet timestamp. Each
    /// payload slice borrows from `packet` and is only valid for that call.
    pub fn parse_packet_with<F: FnMut(u32, &[u8])>(
        &self,
        packet: &[u8],
        sink: &mut F,
    ) -> Result<u64, CodecError> {
        let mut cursor = 0;
        let mut context = vec![0u64; self.packet_def.storage_size];
        let mut timestamp = 0u64;

        for field in &self.packet_def.fields {
            if cursor >= packet.len() {
                break;
            }

            match &field.kind {
                OptFieldType::U8 {
                    const_val: _,
                    mask,
                    shift,
                } => {
                    let mut value = packet[cursor] as u64;
                    if let Some(m) = mask {
                        value &= m;
                    }
                    if let Some(s) = shift {
                        value >>= s;
                    }
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 1;
                }
                OptFieldType::U16Be { .. } => {
                    if cursor + 2 > packet.len() {
                        break;
                    }
                    let value = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 2;
                }
                OptFieldType::U16Le { .. } => {
                    if cursor + 2 > packet.len() {
                        break;
                    }
                    let value = u16::from_le_bytes([packet[cursor], packet[cursor + 1]]) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 2;
                }
                OptFieldType::U32Be { .. } => {
                    if cursor + 4 > packet.len() {
                        break;
                    }
                    let value = u32::from_be_bytes([
                        packet[cursor],
                        packet[cursor + 1],
                        packet[cursor + 2],
                        packet[cursor + 3],
                    ]) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 4;
                }
                OptFieldType::U32Le { .. } => {
                    if cursor + 4 > packet.len() {
                        break;
                    }
                    let value = u32::from_le_bytes([
                        packet[cursor],
                        packet[cursor + 1],
                        packet[cursor + 2],
                        packet[cursor + 3],
                    ]) as u64;
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 4;
                }
                OptFieldType::U64Be => {
                    if cursor + 8 > packet.len() {
                        break;
                    }
                    let value = u64::from_be_bytes([
                        packet[cursor],
                        packet[cursor + 1],
                        packet[cursor + 2],
                        packet[cursor + 3],
                        packet[cursor + 4],
                        packet[cursor + 5],
                        packet[cursor + 6],
                        packet[cursor + 7],
                    ]);
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 8;
                }
                OptFieldType::U64Le => {
                    if cursor + 8 > packet.len() {
                        break;
                    }
                    let value = u64::from_le_bytes([
                        packet[cursor],
                        packet[cursor + 1],
                        packet[cursor + 2],
                        packet[cursor + 3],
                        packet[cursor + 4],
                        packet[cursor + 5],
                        packet[cursor + 6],
                        packet[cursor + 7],
                    ]);
                    if let Some(idx) = field.store_index {
                        context[idx] = value;
                    }
                    if field.is_ts {
                        timestamp = value;
                    }
                    cursor += 8;
                }
                OptFieldType::Sequence {
                    length_ref_index,
                    item_def,
                } => {
                    let seq_len = context[*length_ref_index] as usize;
                    let seq_end = cursor.saturating_add(seq_len).min(packet.len());

                    if let Some(def) = item_def {
                        self.parse_frames_into(&packet[cursor..seq_end], def, sink)?;
                    }
                    cursor = seq_end;
                }
                OptFieldType::Bytes {
                    length_ref_index, ..
                } => {
                    let len = context[*length_ref_index] as usize;
                    cursor += len.min(packet.len() - cursor);
                }
            }
        }

        Ok(timestamp)
    }

    /// Parse frames from a sequence buffer, invoking `sink(can_id, payload)` for
    /// each frame without allocating a per-frame payload buffer. Each payload
    /// slice borrows from `buffer` and is only valid for that invocation.
    fn parse_frames_into<F: FnMut(u32, &[u8])>(
        &self,
        buffer: &[u8],
        frame_type: &OptTypeDef,
        sink: &mut F,
    ) -> Result<(), CodecError> {
        let mut cursor = 0;

        // Pre-allocate context once, reuse for each frame
        let mut context = vec![0u64; frame_type.storage_size];

        while cursor < buffer.len() {
            // Skip padding (0x00 bytes)
            while cursor < buffer.len() && buffer[cursor] == 0 {
                cursor += 1;
            }
            if cursor >= buffer.len() {
                break;
            }

            let mut frame_cursor = cursor;
            // Reset context for this frame
            context.fill(0);
            let mut can_id: Option<u32> = None;
            let mut payload_start = 0usize;
            let mut payload_len = 0usize;
            let mut magic_ok = true;

            for field in &frame_type.fields {
                if frame_cursor >= buffer.len() {
                    break;
                }

                match &field.kind {
                    OptFieldType::U8 {
                        const_val,
                        mask,
                        shift,
                    } => {
                        let mut value = buffer[frame_cursor] as u64;
                        if let Some(m) = mask {
                            value &= m;
                        }
                        if let Some(s) = shift {
                            value >>= s;
                        }

                        if let Some(expected) = const_val
                            && value != *expected
                        {
                            magic_ok = false;
                            break;
                        }
                        if let Some(idx) = field.store_index {
                            context[idx] = value;
                        }
                        frame_cursor += 1;
                    }
                    OptFieldType::U16Be { id_ref } => {
                        if frame_cursor + 2 > buffer.len() {
                            break;
                        }
                        let value =
                            u16::from_be_bytes([buffer[frame_cursor], buffer[frame_cursor + 1]])
                                as u64;
                        if let Some(idx) = field.store_index {
                            context[idx] = value;
                        }
                        if *id_ref {
                            can_id = Some(value as u32);
                        }
                        frame_cursor += 2;
                    }
                    OptFieldType::U32Be { id_ref } => {
                        if frame_cursor + 4 > buffer.len() {
                            break;
                        }
                        let value = u32::from_be_bytes([
                            buffer[frame_cursor],
                            buffer[frame_cursor + 1],
                            buffer[frame_cursor + 2],
                            buffer[frame_cursor + 3],
                        ]) as u64;
                        if let Some(idx) = field.store_index {
                            context[idx] = value;
                        }
                        if *id_ref {
                            can_id = Some(value as u32);
                        }
                        frame_cursor += 4;
                    }
                    OptFieldType::U16Le { id_ref } => {
                        if frame_cursor + 2 > buffer.len() {
                            break;
                        }
                        let value =
                            u16::from_le_bytes([buffer[frame_cursor], buffer[frame_cursor + 1]])
                                as u64;
                        if let Some(idx) = field.store_index {
                            context[idx] = value;
                        }
                        if *id_ref {
                            can_id = Some(value as u32);
                        }
                        frame_cursor += 2;
                    }
                    OptFieldType::U32Le { id_ref } => {
                        if frame_cursor + 4 > buffer.len() {
                            break;
                        }
                        let value = u32::from_le_bytes([
                            buffer[frame_cursor],
                            buffer[frame_cursor + 1],
                            buffer[frame_cursor + 2],
                            buffer[frame_cursor + 3],
                        ]) as u64;
                        if let Some(idx) = field.store_index {
                            context[idx] = value;
                        }
                        if *id_ref {
                            can_id = Some(value as u32);
                        }
                        frame_cursor += 4;
                    }
                    OptFieldType::U64Be => {
                        if frame_cursor + 8 > buffer.len() {
                            break;
                        }
                        let value = u64::from_be_bytes([
                            buffer[frame_cursor],
                            buffer[frame_cursor + 1],
                            buffer[frame_cursor + 2],
                            buffer[frame_cursor + 3],
                            buffer[frame_cursor + 4],
                            buffer[frame_cursor + 5],
                            buffer[frame_cursor + 6],
                            buffer[frame_cursor + 7],
                        ]);
                        if let Some(idx) = field.store_index {
                            context[idx] = value;
                        }
                        frame_cursor += 8;
                    }
                    OptFieldType::U64Le => {
                        if frame_cursor + 8 > buffer.len() {
                            break;
                        }
                        let value = u64::from_le_bytes([
                            buffer[frame_cursor],
                            buffer[frame_cursor + 1],
                            buffer[frame_cursor + 2],
                            buffer[frame_cursor + 3],
                            buffer[frame_cursor + 4],
                            buffer[frame_cursor + 5],
                            buffer[frame_cursor + 6],
                            buffer[frame_cursor + 7],
                        ]);
                        if let Some(idx) = field.store_index {
                            context[idx] = value;
                        }
                        frame_cursor += 8;
                    }
                    OptFieldType::Bytes {
                        length_ref_index,
                        format_id_ref_index,
                    } => {
                        let len = context[*length_ref_index] as usize;
                        let clamped_len = len.min(buffer.len() - frame_cursor);

                        // Only capture payload boundaries for the bytes field that carries
                        // format.id_ref. Raw sibling bytes fields (e.g. checksum, metadata)
                        // still advance frame_cursor but must not overwrite the payload bounds.
                        if let Some(id_idx) = format_id_ref_index {
                            payload_start = frame_cursor;
                            payload_len = clamped_len;
                            can_id = Some(context[*id_idx] as u32);
                        }

                        frame_cursor += clamped_len;
                    }
                    _ => {}
                }
            }

            if !magic_ok {
                cursor += 1;
                continue;
            }

            if frame_cursor > cursor
                && let Some(cid) = can_id
            {
                sink(cid, &buffer[payload_start..payload_start + payload_len]);
            }

            if frame_cursor == cursor {
                break;
            }
            cursor = frame_cursor;
        }

        Ok(())
    }

    /// Calculate the size of fixed header fields before the sequence.
    fn calculate_header_size(&self, type_def: &OptTypeDef) -> usize {
        let mut size = 0;
        for field in &type_def.fields {
            match field.kind {
                OptFieldType::U8 { .. } => size += 1,
                OptFieldType::U16Be { .. } | OptFieldType::U16Le { .. } => size += 2,
                OptFieldType::U32Be { .. } | OptFieldType::U32Le { .. } => size += 4,
                OptFieldType::U64Be | OptFieldType::U64Le => size += 8,
                OptFieldType::Sequence { .. } | OptFieldType::Bytes { .. } => break,
            }
        }
        size
    }

    /// Find the offset and size of the length field.
    fn find_length_field_offset(&self, type_def: &OptTypeDef) -> Option<(usize, usize)> {
        let mut offset = 0;
        // Find which field is used as length ref for the sequence
        let length_ref_idx = type_def
            .fields
            .iter()
            .filter_map(|f| match &f.kind {
                OptFieldType::Sequence {
                    length_ref_index, ..
                } => Some(*length_ref_index),
                _ => None,
            })
            .next();

        if let Some(target_idx) = length_ref_idx {
            for field in &type_def.fields {
                let size = match field.kind {
                    OptFieldType::U8 { .. } => 1,
                    OptFieldType::U16Be { .. } => 2,
                    OptFieldType::U32Be { .. } => 4,
                    OptFieldType::U64Be => 8,
                    _ => return None,
                };

                if field.index == target_idx {
                    return Some((offset, size));
                }
                offset += size;
            }
        }
        None
    }

    /// Parse an unsigned integer from bytes.
    fn parse_uint(&self, bytes: &[u8], size: usize) -> u64 {
        match size {
            1 => bytes[0] as u64,
            2 => u16::from_be_bytes([bytes[0], bytes[1]]) as u64,
            4 => u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64,
            8 => u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::gbf::GbfSchema;

    fn get_standard_schema() -> GbfSchema {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    { 
                        "name": "frames", 
                        "type": "sequence", 
                        "length_ref": "total_len", 
                        "structure": { 
                            "type": "struct",
                            "fields": [
                                { "name": "magic", "type": "u8", "const": 85 },
                                { "name": "can_id", "type": "u16be" },
                                { "name": "data_len", "type": "u8" },
                                { "name": "payload", "type": "bytes", "length_ref": "data_len", "format": { "id_ref": "can_id" } }
                            ]
                        }
                    }
                ]
            }
        }
        "#;
        serde_json::from_str(json).expect("parse schema")
    }

    // Test Issue #1: Timestamp detection should prioritize name over type
    #[test]
    fn test_timestamp_detection_prioritizes_name_over_type() {
        // Schema where total_len (u32) comes before timestamp
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "total_len", "type": "u32be" },
                    { "name": "ts", "type": "u64be" },
                    { 
                        "name": "frames", 
                        "type": "sequence", 
                        "length_ref": "total_len", 
                        "structure": { 
                            "type": "struct",
                            "fields": [
                                { "name": "id", "type": "u16be" },
                                { "name": "data", "type": "bytes", "length_ref": "id", "format": { "id_ref": "id" } }
                            ]
                        } 
                    }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let field_map = SchemaFieldMap::from_schema(&schema).expect("field map");

        // Should detect "ts" as timestamp, not "total_len" (even though total_len is first u32)
        assert_eq!(field_map.timestamp_field, "ts");
    }

    // Test that non-ts/timestamp field names are rejected
    #[test]
    fn test_timestamp_requires_ts_or_timestamp_name() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "epoch", "type": "u64be" },
                    { "name": "count", "type": "u16be" },
                    { 
                        "name": "items", 
                        "type": "sequence", 
                        "length_ref": "count", 
                        "structure": { 
                            "type": "struct",
                            "fields": [
                                { "name": "id", "type": "u16be" },
                                { "name": "data", "type": "bytes", "length_ref": "id", "format": { "id_ref": "id" } }
                            ]
                        } 
                    }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let result = SchemaFieldMap::from_schema(&schema);

        // Should error because "epoch" is not "ts" or "timestamp"
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ts"));
    }

    // Test Issue #2: Empty packets handling
    #[test]
    fn test_split_packets_empty_payload() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema).expect("create parser");

        let packets = parser.split_packets(&[]);
        assert!(packets.is_empty());
    }

    // Test Issue #2: Payload smaller than header
    #[test]
    fn test_split_packets_payload_smaller_than_header() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema).expect("create parser");

        // Only 5 bytes, but header needs 10 (8 for ts + 2 for total_len)
        let packets = parser.split_packets(&[0, 0, 0, 0, 0]);
        assert!(packets.is_empty());
    }

    // Test parse_packet with valid data
    #[test]
    fn test_parse_packet_extracts_timestamp_and_frames() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema).expect("create parser");

        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 100]); // ts = 100
        packet.extend_from_slice(&[0, 12]); // total_len = 12
        packet.push(0x55); // magic
        packet.extend_from_slice(&[0x01, 0x00]); // can_id = 256
        packet.push(0x08); // data_len = 8
        packet.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]); // payload

        let (timestamp, frames) = parser.parse_packet(&packet).expect("parse");
        assert_eq!(timestamp, 100);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].can_id, 256);
        assert_eq!(frames[0].payload, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    // Test parse_packet with no frames (heartbeat)
    #[test]
    fn test_parse_packet_heartbeat_no_frames() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema).expect("create parser");

        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 50]); // ts = 50
        packet.extend_from_slice(&[0, 0]); // total_len = 0 (no frames)

        let (timestamp, frames) = parser.parse_packet(&packet).expect("parse");
        assert_eq!(timestamp, 50);
        assert!(frames.is_empty());
    }

    // Test field_map accessor
    #[test]
    fn test_field_map_returns_correct_mappings() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema).expect("create parser");
        let map = parser.field_map();

        assert_eq!(map.timestamp_field, "ts");
        assert_eq!(map.sequence_field, "frames");
        assert_eq!(map.sequence_item_type, "struct");
        assert_eq!(map.frame_id_field, "can_id");
        assert_eq!(map.frame_payload_field, "payload");
    }

    // Test multiple packets in single payload
    #[test]
    fn test_split_multiple_packets() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema).expect("create parser");

        let mut payload = Vec::new();
        // Packet 1
        payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]); // ts = 1
        payload.extend_from_slice(&[0, 0]); // total_len = 0
        // Packet 2
        payload.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 2]); // ts = 2
        payload.extend_from_slice(&[0, 0]); // total_len = 0

        let packets = parser.split_packets(&payload);
        assert_eq!(packets.len(), 2);
    }

    // Test invalid magic byte skipped
    #[test]
    fn test_parse_packet_skips_invalid_magic() {
        let schema = get_standard_schema();
        let parser = GbfParser::new(schema).expect("create parser");

        let mut packet = Vec::new();
        packet.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 100]); // ts
        packet.extend_from_slice(&[0, 12]); // total_len
        packet.push(0xAA); // WRONG magic (not 0x55)
        packet.extend_from_slice(&[0x01, 0x00]); // can_id
        packet.push(0x08); // data_len
        packet.extend_from_slice(&[0; 8]); // payload

        let (_, frames) = parser.parse_packet(&packet).expect("parse");
        // Frame with wrong magic should be skipped
        assert!(frames.is_empty());
    }

    // Test that a schema with no root 'structure' field is rejected at deserialization time.
    #[test]
    fn test_parser_rejects_missing_root_structure() {
        let json = r#"{ "other": {} }"#;
        let result: Result<GbfSchema, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "expected serde to reject a schema missing the 'structure' field"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("structure"),
            "expected error to mention 'structure', got: {err}"
        );
    }

    // Test that GbfParser::new() rejects a sequence field with no nested 'structure'.
    #[test]
    fn test_parser_rejects_sequence_missing_structure() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    { "name": "frames", "type": "sequence", "length_ref": "total_len" }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        match GbfParser::new(schema) {
            Err(e) => assert!(
                e.to_string().to_lowercase().contains("missing structure"),
                "unexpected error: {e}"
            ),
            Ok(_) => panic!("expected an error for sequence missing structure"),
        }
    }

    // Test SchemaFieldMap error cases
    #[test]
    fn test_schema_field_map_no_sequence_error() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let result = SchemaFieldMap::from_schema(&schema);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No sequence field")
        );
    }

    // Test that field names containing "time" but not matching "ts"/"timestamp" exactly
    // are NOT flagged as the timestamp field.
    #[test]
    fn test_is_ts_not_matched_by_time_substring() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "uptime", "type": "u64be" },
                    { "name": "ts", "type": "u64be" },
                    { "name": "total_len", "type": "u16be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "total_len",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "id", "type": "u16be" },
                                { "name": "data", "type": "bytes", "length_ref": "id", "format": { "id_ref": "id" } }
                            ]
                        }
                    }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let parser = GbfParser::new(schema).expect("create parser");

        // Build a packet: uptime=999, ts=42, total_len=0
        let mut packet = Vec::new();
        packet.extend_from_slice(&999u64.to_be_bytes()); // uptime
        packet.extend_from_slice(&42u64.to_be_bytes()); // ts
        packet.extend_from_slice(&0u16.to_be_bytes()); // total_len

        let (timestamp, _) = parser.parse_packet(&packet).expect("parse");
        // The timestamp should be 42 (from "ts"), not 999 (from "uptime")
        assert_eq!(timestamp, 42);
    }

    // Test that a root-level bytes field advances cursor correctly so the
    // sequence field that follows parses from the right offset.
    #[test]
    fn test_parser_rejects_bytes_field_before_sequence_length() {
        // A schema with a bytes field before the sequence length field is rejected at
        // compile time (GbfParser::new), because split_packets() cannot compute the
        // offset to the length field when a variable-length field precedes it.
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "hdr_len", "type": "u8" },
                    { "name": "header", "type": "bytes", "length_ref": "hdr_len" },
                    { "name": "seq_len", "type": "u16be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "seq_len",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "can_id", "type": "u16be" },
                                { "name": "dlen", "type": "u8" },
                                { "name": "payload", "type": "bytes", "length_ref": "dlen", "format": { "id_ref": "can_id" } }
                            ]
                        }
                    }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let result = GbfParser::new(schema);
        assert!(
            result.is_err(),
            "should reject bytes field before length field"
        );
        let msg = result.err().unwrap().to_string();
        assert!(
            msg.contains("bytes field 'header'") && msg.contains("seq_len"),
            "unexpected error: {msg}"
        );
    }

    // Test that frame items with u64be fields have their cursor advanced correctly.
    #[test]
    fn test_parse_frames_u64be_field_advances_cursor() {
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "seq_len", "type": "u16be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "seq_len",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "epoch", "type": "u64be" },
                                { "name": "can_id", "type": "u16be" },
                                { "name": "dlen", "type": "u8" },
                                { "name": "payload", "type": "bytes", "length_ref": "dlen", "format": { "id_ref": "can_id" } }
                            ]
                        }
                    }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse");
        let parser = GbfParser::new(schema).expect("create parser");

        let frame_epoch: u64 = 0x0102030405060708;
        let mut packet = Vec::new();
        packet.extend_from_slice(&50u64.to_be_bytes()); // ts = 50
        // frame: epoch(8) + can_id(2) + dlen(1) + payload(2) = 13
        packet.extend_from_slice(&13u16.to_be_bytes()); // seq_len = 13
        packet.extend_from_slice(&frame_epoch.to_be_bytes()); // epoch
        packet.extend_from_slice(&0x0300u16.to_be_bytes()); // can_id
        packet.push(2); // dlen
        packet.extend_from_slice(&[0xDE, 0xAD]); // payload

        let (timestamp, frames) = parser.parse_packet(&packet).expect("parse");
        assert_eq!(timestamp, 50);
        assert_eq!(frames.len(), 1, "u64be frame field must advance cursor");
        assert_eq!(frames[0].can_id, 0x0300);
        assert_eq!(frames[0].payload, vec![0xDE, 0xAD]);
    }

    // Regression test: a frame struct with both a formatted payload field and a raw bytes
    // sibling (e.g. checksum) must capture only the payload field's boundaries.
    // The raw bytes sibling must advance the cursor but must NOT overwrite payload_start /
    // payload_len, regardless of whether it appears before or after the payload field.
    #[test]
    fn test_parse_frames_raw_bytes_sibling_does_not_overwrite_payload() {
        // Schema: frame has can_id, dlen, csum_len, payload (format), checksum (no format).
        // The checksum field appears AFTER the payload field, which previously caused it to
        // overwrite the payload boundaries.
        let json = r#"
        {
            "structure": {
                "type": "struct",
                "fields": [
                    { "name": "ts", "type": "u64be" },
                    { "name": "seq_len", "type": "u16be" },
                    {
                        "name": "frames",
                        "type": "sequence",
                        "length_ref": "seq_len",
                        "structure": {
                            "type": "struct",
                            "fields": [
                                { "name": "can_id", "type": "u16be" },
                                { "name": "dlen", "type": "u8" },
                                { "name": "csum_len", "type": "u8" },
                                { "name": "payload", "type": "bytes", "length_ref": "dlen", "format": { "id_ref": "can_id" } },
                                { "name": "checksum", "type": "bytes", "length_ref": "csum_len" }
                            ]
                        }
                    }
                ]
            }
        }
        "#;
        let schema: GbfSchema = serde_json::from_str(json).expect("parse schema");
        let parser = GbfParser::new(schema).expect("create parser");

        let can_id: u16 = 0x0102;
        let payload_data: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        let checksum_data: &[u8] = &[0xAB, 0xCD];
        // frame size: can_id(2) + dlen(1) + csum_len(1) + payload(4) + checksum(2) = 10
        let seq_len: u16 = 10;

        let mut packet = Vec::new();
        packet.extend_from_slice(&42u64.to_be_bytes()); // ts
        packet.extend_from_slice(&seq_len.to_be_bytes()); // seq_len
        packet.extend_from_slice(&can_id.to_be_bytes()); // can_id
        packet.push(payload_data.len() as u8); // dlen
        packet.push(checksum_data.len() as u8); // csum_len
        packet.extend_from_slice(payload_data); // payload
        packet.extend_from_slice(checksum_data); // checksum (raw sibling)

        let (timestamp, frames) = parser.parse_packet(&packet).expect("parse");
        assert_eq!(timestamp, 42);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].can_id, u32::from(can_id));
        assert_eq!(
            frames[0].payload, payload_data,
            "payload must not be overwritten by the raw checksum sibling bytes field"
        );
    }
}
