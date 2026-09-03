//! Packed CAN identity used as the DBC lookup key.
//!
//! The unique key is a Vector/SocketCAN `u32`: bit 31 is the extended-frame
//! flag (IDE), and bits 0–28 are the CAN ID. BusMirror always composes this
//! key from `FrameIDCAN` (keeping IDE, dropping FD/reserved). GBF does the
//! same only when `extend_ref` is set; otherwise `id_ref` must already be
//! this packed `u32`.

/// Vector/SocketCAN extended-frame flag (IDE).
pub const CAN_EFF_FLAG: u32 = 0x8000_0000;
/// 29-bit CAN identifier mask.
pub const CAN_ID_MASK: u32 = 0x1FFF_FFFF;

/// Build the DBC `u32` lookup key from an IDE flag and a raw CAN ID.
#[inline]
pub const fn packed_can_id(extended: bool, raw_id: u32) -> u32 {
    let id = raw_id & CAN_ID_MASK;
    if extended { CAN_EFF_FLAG | id } else { id }
}

/// AUTOSAR BusMirror `FrameIDCAN` → DBC `u32` key.
///
/// Keep IDE (bit 31). Drop FD (bit 30) and reserved (bit 29).
#[inline]
pub const fn busmirror_can_id(wire: u32) -> u32 {
    packed_can_id(wire & CAN_EFF_FLAG != 0, wire)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_can_id_sets_ide_only_when_extended() {
        assert_eq!(packed_can_id(false, 0x123), 0x123);
        assert_eq!(packed_can_id(true, 0x123), CAN_EFF_FLAG | 0x123);
        assert_eq!(
            packed_can_id(true, CAN_EFF_FLAG | 0x123),
            CAN_EFF_FLAG | 0x123
        );
    }

    #[test]
    fn busmirror_can_id_keeps_ide_and_clears_fd() {
        assert_eq!(busmirror_can_id(0x0000_0100), 0x0000_0100);
        assert_eq!(busmirror_can_id(0x4000_0100), 0x0000_0100);
        assert_eq!(busmirror_can_id(0x8000_0100), 0x8000_0100);
        assert_eq!(busmirror_can_id(0xC000_0123), CAN_EFF_FLAG | 0x123);
    }
}
