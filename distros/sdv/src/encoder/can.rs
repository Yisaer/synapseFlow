//! CAN signal encoder for packing signal values into CAN frame payloads.
//!
//! This module provides encoding of CAN signals based on DBC definitions,
//! packing signal values into 8-byte CAN payloads using proper bit positioning.

use crate::schema::dbc::{DbcJson, MessageJson, SignalJson};
use rand::Rng;
use std::collections::HashMap;

/// Error type for CAN encoding operations.
#[derive(Debug)]
pub enum CanEncodeError {
    /// Message ID not found in DBC schema.
    MessageNotFound(u32),
    /// Signal not found in message.
    SignalNotFound(String),
    /// Signal value out of range.
    ValueOutOfRange { signal: String, value: i64 },
}

impl std::fmt::Display for CanEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CanEncodeError::MessageNotFound(id) => {
                write!(f, "CAN message ID 0x{:X} not found in DBC", id)
            }
            CanEncodeError::SignalNotFound(name) => write!(f, "signal '{}' not found", name),
            CanEncodeError::ValueOutOfRange { signal, value } => {
                write!(f, "signal '{}' value {} out of range", signal, value)
            }
        }
    }
}

impl std::error::Error for CanEncodeError {}

/// CAN signal encoder that packs signal values into frame payloads based on DBC schema.
pub struct CanEncoder {
    /// Message definitions indexed by CAN ID for fast lookup.
    messages: HashMap<u32, MessageJson>,
}

impl CanEncoder {
    /// Create a new CAN encoder from a DBC schema.
    pub fn new(dbc: &DbcJson) -> Self {
        let mut messages = HashMap::new();
        for bus in &dbc.buses {
            for msg in &bus.messages {
                messages.insert(msg.id, msg.clone());
            }
        }
        Self { messages }
    }

    /// Encode signal values into an 8-byte CAN payload.
    ///
    /// # Arguments
    /// * `can_id` - CAN message ID
    /// * `signal_values` - Map of signal names to raw values (before scaling)
    ///
    /// # Returns
    /// 8-byte payload with signals packed according to DBC definition.
    pub fn encode(
        &self,
        can_id: u32,
        signal_values: &HashMap<String, i64>,
    ) -> Result<Vec<u8>, CanEncodeError> {
        let msg = self
            .messages
            .get(&can_id)
            .ok_or(CanEncodeError::MessageNotFound(can_id))?;

        let mut payload = vec![0u8; 8];

        for signal in &msg.signals {
            if let Some(&value) = signal_values.get(&signal.name) {
                pack_signal(&mut payload, signal, value);
            }
        }

        Ok(payload)
    }

    /// Generate random signal values for a CAN message.
    ///
    /// Returns raw values (not physical values - no scaling applied).
    pub fn generate_random<R: Rng>(
        &self,
        can_id: u32,
        rng: &mut R,
    ) -> Result<HashMap<String, i64>, CanEncodeError> {
        let msg = self
            .messages
            .get(&can_id)
            .ok_or(CanEncodeError::MessageNotFound(can_id))?;

        let mut values = HashMap::new();
        for signal in &msg.signals {
            // Skip multiplexed signals for now
            if signal.is_multiplexed {
                continue;
            }

            let max_val = (1i64 << signal.length) - 1;
            let value = if signal.is_signed {
                // Generate signed value in range
                let half = max_val / 2;
                rng.gen_range(-half..=half)
            } else {
                rng.gen_range(0..=max_val)
            };
            values.insert(signal.name.clone(), value);
        }

        Ok(values)
    }

    /// Get all available CAN IDs from the DBC schema.
    pub fn message_ids(&self) -> Vec<u32> {
        self.messages.keys().copied().collect()
    }

    /// Check if a CAN ID exists in the schema.
    pub fn has_message(&self, can_id: u32) -> bool {
        self.messages.contains_key(&can_id)
    }
}

/// Pack a signal value into a CAN payload at the correct bit position.
fn pack_signal(payload: &mut [u8], signal: &SignalJson, value: i64) {
    let start_bit = signal.start as usize;
    let length = signal.length as usize;

    // Mask value to fit in signal length
    let mask = if length >= 64 {
        u64::MAX
    } else {
        (1u64 << length) - 1
    };
    let value = (value as u64) & mask;

    if signal.is_big_endian {
        // Big-endian (Motorola): MSB at start_bit
        pack_big_endian(payload, start_bit, length, value);
    } else {
        // Little-endian (Intel): LSB at start_bit
        pack_little_endian(payload, start_bit, length, value);
    }
}

/// Pack a value in little-endian (Intel) format.
/// Start bit is the LSB position.
fn pack_little_endian(payload: &mut [u8], start_bit: usize, length: usize, value: u64) {
    let mut remaining_bits = length;
    let mut bit_pos = start_bit;
    let mut val = value;

    while remaining_bits > 0 {
        let byte_idx = bit_pos / 8;
        let bit_in_byte = bit_pos % 8;

        if byte_idx >= payload.len() {
            break;
        }

        // How many bits can we put in this byte?
        let bits_available = 8 - bit_in_byte;
        let bits_to_write = remaining_bits.min(bits_available);

        // Create mask for bits to write
        let mask = ((1u64 << bits_to_write) - 1) as u8;
        let byte_val = ((val & mask as u64) as u8) << bit_in_byte;

        payload[byte_idx] |= byte_val;

        val >>= bits_to_write;
        remaining_bits -= bits_to_write;
        bit_pos += bits_to_write;
    }
}

/// Pack a value in big-endian (Motorola) format.
/// Start bit is the MSB position in DBC format.
fn pack_big_endian(payload: &mut [u8], start_bit: usize, length: usize, value: u64) {
    // In DBC big-endian format, start_bit is the MSB position
    // The signal spans from start_bit (MSB) going down and wrapping to next bytes

    if length == 0 || length > 64 {
        return;
    }

    // For signals up to 8 bits in a single byte, use simple approach
    let start_byte = start_bit / 8;
    let start_bit_in_byte = start_bit % 8;

    if length <= start_bit_in_byte + 1 && start_byte < payload.len() {
        // Signal fits in one byte
        let shift = start_bit_in_byte + 1 - length;
        let mask = ((1u64 << length) - 1) as u8;
        payload[start_byte] |= ((value as u8) & mask) << shift;
        return;
    }

    // For multi-byte signals, pack byte by byte
    let mut remaining_bits = length;
    let mut byte_idx = start_byte;
    let mut bit_pos = start_bit_in_byte;
    let mut val = value;

    // Process from MSB to LSB
    while remaining_bits > 0 && byte_idx < payload.len() {
        // Bits we can write in this byte (from bit_pos down to 0)
        let bits_in_this_byte = (bit_pos + 1).min(remaining_bits);

        // Extract the top bits_in_this_byte bits from the remaining value
        let shift_for_extract = remaining_bits.saturating_sub(bits_in_this_byte);
        let extracted = if shift_for_extract < 64 {
            (val >> shift_for_extract) as u8
        } else {
            0
        };

        // Create mask for the bits we're writing
        let mask = if bits_in_this_byte >= 8 {
            0xFF
        } else {
            (1u8 << bits_in_this_byte) - 1
        };
        let byte_val = extracted & mask;

        // Position in the byte
        let shift_amount = bit_pos + 1 - bits_in_this_byte;
        payload[byte_idx] |= byte_val << shift_amount;

        // Clear the bits we just used from val
        if shift_for_extract < 64 {
            val &= (1u64 << shift_for_extract) - 1;
        }

        remaining_bits -= bits_in_this_byte;
        byte_idx += 1;
        bit_pos = 7; // Next byte starts from bit 7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::dbc::{BusJson, DbcJson, MessageJson, SignalJson};

    fn create_test_dbc() -> DbcJson {
        DbcJson {
            buses: vec![BusJson {
                name: Some("Test".to_string()),
                id: 0,
                messages: vec![MessageJson {
                    _name: "TestMsg".to_string(),
                    id: 0x100,
                    frame_id: "0x100".to_string(),
                    _length: 8,
                    signals: vec![
                        SignalJson {
                            name: "Sig1".to_string(),
                            start: 0,
                            length: 8,
                            scale: Some(1.0),
                            offset: Some(0.0),
                            is_big_endian: false,
                            is_signed: false,
                            is_multiplexer: false,
                            is_multiplexed: false,
                            multiplexer_value: None,
                            min: None,
                            max: None,
                        },
                        SignalJson {
                            name: "Sig2".to_string(),
                            start: 8,
                            length: 16,
                            scale: Some(1.0),
                            offset: Some(0.0),
                            is_big_endian: false,
                            is_signed: false,
                            is_multiplexer: false,
                            is_multiplexed: false,
                            multiplexer_value: None,
                            min: None,
                            max: None,
                        },
                    ],
                }],
            }],
        }
    }

    #[test]
    fn test_encode_simple_signals() {
        let dbc = create_test_dbc();
        let encoder = CanEncoder::new(&dbc);

        let mut values = HashMap::new();
        values.insert("Sig1".to_string(), 0x42);
        values.insert("Sig2".to_string(), 0x1234);

        let payload = encoder.encode(0x100, &values).expect("encode");

        assert_eq!(payload[0], 0x42); // Sig1 at byte 0
        assert_eq!(payload[1], 0x34); // Sig2 low byte
        assert_eq!(payload[2], 0x12); // Sig2 high byte
    }

    #[test]
    fn test_generate_random() {
        let dbc = create_test_dbc();
        let encoder = CanEncoder::new(&dbc);
        let mut rng = rand::thread_rng();

        let values = encoder.generate_random(0x100, &mut rng).expect("random");

        assert!(values.contains_key("Sig1"));
        assert!(values.contains_key("Sig2"));

        // Values should be within range
        assert!(*values.get("Sig1").unwrap() <= 255);
        assert!(*values.get("Sig2").unwrap() <= 65535);
    }

    #[test]
    fn test_message_not_found() {
        let dbc = create_test_dbc();
        let encoder = CanEncoder::new(&dbc);

        let result = encoder.encode(0x999, &HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_endianness_and_packing() {
        // Create manual definition for testing complex packing
        let mut signals = vec![];

        // Little endian, crossing byte boundary
        // Start bit 4, length 12. Covers byte 0[4..8] and byte 1[0..8] ideally
        // In LE: LSB is at start_bit.
        // Bits 0-3 of value go to Byte 0 [4-7]
        // Bits 4-11 of value go to Byte 1 [0-7]
        signals.push(SignalJson {
            name: "LeSignal".to_string(),
            start: 4,
            length: 12,
            scale: Some(1.0),
            offset: Some(0.0),
            is_big_endian: false,
            is_signed: false,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            min: None,
            max: None,
        });

        // Big endian
        // Start bit 20 (Byte 2 bit 4), length 12
        // In BE: MSB at start_bit.
        // Bits 11-? go to Byte 2 [..4]
        // It goes "backwards" effectively but logical bits are contiguous
        // Let's use a simpler known case.
        // Byte 3 (bits 24-31). Start bit 27 (Byte 3 bit 3). Length 4.
        // Occupies Byte 3 [0-3]
        signals.push(SignalJson {
            name: "BeSignal".to_string(),
            start: 27,
            length: 4,
            scale: Some(1.0),
            offset: Some(0.0),
            is_big_endian: true,
            is_signed: false,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            min: None,
            max: None,
        });

        signals.push(SignalJson {
            name: "BeMultiByte".to_string(),
            start: 15, // Byte 1, bit 7
            length: 10,
            scale: Some(1.0),
            offset: Some(0.0),
            is_big_endian: true, // Big Endian
            is_signed: false,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            min: None,
            max: None,
        });

        let msg = MessageJson {
            _name: "ComplexMsg".to_string(),
            id: 0x200,
            frame_id: "0x200".to_string(),
            _length: 8,
            signals,
        };

        let dbc = DbcJson {
            buses: vec![BusJson {
                name: Some("Test".to_string()),
                id: 0,
                messages: vec![msg],
            }],
        };

        let encoder = CanEncoder::new(&dbc);
        let mut values = HashMap::new();

        // LeSignal: 0xABC (12 bits)
        // 0xC = 1100 binary. Lower 4 bits.
        // 0xAB = 10101011 binary. Upper 8 bits.
        // Byte 0 should have 0xC shifted to bit 4 => 0xC0
        // Byte 1 should have 0xAB
        values.insert("LeSignal".to_string(), 0xABC);

        // BeSignal: 0xA (1010 binary). 4 bits.
        // Start bit 27 is Byte 3 bit 3.
        // Occupies 27, 26, 25, 24 => Byte 3 bits 3,2,1,0.
        // So Byte 3 should be 0xA (0000 1010)
        values.insert("BeSignal".to_string(), 0xA);

        // BeMultiByte: Start 15 (Byte 1 bit 7), Length 10.
        // Byte 1: bits 15..8 (8 bits).
        // Byte 2: bits 7..6 (2 bits).
        // Value: 0x3FF (11 1111 1111)
        // Upper 8 bits (11 1111 11xx) -> 0xFF (since 0x3FF >> 2 is 0xFF)
        // Lower 2 bits (xx xxxx xx11) -> 0x3
        // Byte 1 gets MSB part: 0xFF.
        // Byte 2 gets LSB part: 0x3 formatted into bits 7,6.
        // 0x3 (11 binary) placed at 7,6 => 1100 0000 => 0xC0.
        values.insert("BeMultiByte".to_string(), 0x3FF);

        let payload = encoder.encode(0x200, &values).expect("encode");

        // Check LeSignal
        assert_eq!(payload[0] & 0xF0, 0xC0, "LeSignal low nibble mismatch");
        // Byte 1 is overlapped by BeMultiByte, so it should be 0xFF
        // assert_eq!(payload[1], 0xAB, "LeSignal high byte mismatch"); // Updated below

        // Check BeSignal
        assert_eq!(payload[3] & 0x0F, 0x0A, "BeSignal mismatch");

        // Check BeMultiByte
        // Byte 1 should be 0xFF (OR of 0xAB from LeSignal and 0xFF from BeMultiByte)
        // LeSignal (0xAB) | BeMultiByte (0xFF) = 0xFF
        assert_eq!(
            payload[1], 0xFF,
            "Byte 1 overlap (LeSignal | BeMultiByte) mismatch"
        );
        assert_eq!(payload[2] & 0xC0, 0xC0, "BeMultiByte Byte 2 mismatch");
    }

    #[test]
    fn test_signed_values() {
        let signals = vec![SignalJson {
            name: "Signed8".to_string(),
            start: 0,
            length: 8,
            scale: Some(1.0),
            offset: Some(0.0),
            is_big_endian: false,
            is_signed: true,
            is_multiplexer: false,
            is_multiplexed: false,
            multiplexer_value: None,
            min: None,
            max: None,
        }];

        let msg = MessageJson {
            _name: "SignedMsg".to_string(),
            id: 0x300,
            frame_id: "0x300".to_string(),
            _length: 8,
            signals,
        };

        let dbc = DbcJson {
            buses: vec![BusJson {
                name: Some("Test".to_string()),
                id: 0,
                messages: vec![msg],
            }],
        };

        let encoder = CanEncoder::new(&dbc);
        let mut values = HashMap::new();

        // -1 formatted as 8-bit two's complement is 0xFF
        values.insert("Signed8".to_string(), -1);
        let payload = encoder.encode(0x300, &values).expect("encode");
        assert_eq!(payload[0], 0xFF);

        // -128 is 0x80
        values.insert("Signed8".to_string(), -128);
        let payload = encoder.encode(0x300, &values).expect("encode");
        assert_eq!(payload[0], 0x80);
    }

    #[test]
    fn test_random_with_multiplexed() {
        let signals = vec![
            SignalJson {
                name: "Normal".to_string(),
                start: 0,
                length: 8,
                scale: Some(1.0),
                offset: Some(0.0),
                is_big_endian: false,
                is_signed: false,
                is_multiplexer: false,
                is_multiplexed: false,
                multiplexer_value: None,
                min: None,
                max: None,
            },
            SignalJson {
                name: "Multiplexed".to_string(),
                start: 8,
                length: 8,
                scale: Some(1.0),
                offset: Some(0.0),
                is_big_endian: false,
                is_signed: false,
                is_multiplexer: false,
                is_multiplexed: true, // Should be skipped
                multiplexer_value: Some(1),
                min: None,
                max: None,
            },
        ];

        let msg = MessageJson {
            _name: "MuxMsg".to_string(),
            id: 0x400,
            frame_id: "0x400".to_string(),
            _length: 8,
            signals,
        };

        let dbc = DbcJson {
            buses: vec![BusJson {
                name: Some("Test".to_string()),
                id: 0,
                messages: vec![msg],
            }],
        };

        let encoder = CanEncoder::new(&dbc);
        let mut rng = rand::thread_rng();

        let values = encoder.generate_random(0x400, &mut rng).expect("random");
        assert!(values.contains_key("Normal"));
        assert!(!values.contains_key("Multiplexed"));
    }

    #[test]
    fn test_api_coverage() {
        let dbc = create_test_dbc();
        let encoder = CanEncoder::new(&dbc);

        let ids = encoder.message_ids();
        assert_eq!(ids, vec![0x100]);

        assert!(encoder.has_message(0x100));
        assert!(!encoder.has_message(0x999));
    }

    #[test]
    fn test_error_display() {
        let err1 = CanEncodeError::MessageNotFound(0x123);
        assert_eq!(format!("{}", err1), "CAN message ID 0x123 not found in DBC");

        let err2 = CanEncodeError::SignalNotFound("Foo".to_string());
        assert_eq!(format!("{}", err2), "signal 'Foo' not found");

        let err3 = CanEncodeError::ValueOutOfRange {
            signal: "Bar".to_string(),
            value: 100,
        };
        assert_eq!(format!("{}", err3), "signal 'Bar' value 100 out of range");
    }
}
