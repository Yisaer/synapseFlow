//! Compiled signal-name patterns shared by schema compilers.

use std::fmt::Write;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum NameToken {
    BusName,
    BusIdDecimal,
    BusIdHexLower,
    BusIdHexUpper,
    MessageIdDecimal,
    MessageIdHexLower,
    MessageIdHexUpper,
    MessageName,
    SignalName,
    NetworkType,
    NetworkTypeIdDecimal,
    NetworkTypeIdHexLower,
    NetworkTypeIdHexUpper,
    NetworkIdDecimal,
    NetworkIdHexLower,
    NetworkIdHexUpper,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbcNamePatternMode {
    Standard,
    BusMirror,
}

#[derive(Debug, Clone)]
enum PatternPart {
    Literal(Arc<str>),
    Token(NameToken),
}

/// Values available while rendering one DBC signal name.
pub struct DbcNameContext<'a> {
    pub bus_name: &'a str,
    pub bus_id: u32,
    pub message_id: u32,
    pub message_name: &'a str,
    pub signal_name: &'a str,
    pub network: Option<NetworkNameContext<'a>>,
}

/// BusMirror-specific values exposed to a signal-name pattern.
#[derive(Clone, Copy)]
pub struct NetworkNameContext<'a> {
    pub network_type: &'a str,
    pub network_type_id: u8,
    pub network_id: u8,
}

/// A validated name pattern represented as literal and token parts.
#[derive(Debug, Clone)]
pub struct CompiledNamePattern {
    parts: Arc<[PatternPart]>,
}

impl CompiledNamePattern {
    /// Compile a DBC signal-name pattern.
    ///
    /// Network tokens are accepted only for BusMirror schemas, whose naming
    /// context contains a network type and network ID.
    pub fn compile(pattern: &str, mode: DbcNamePatternMode) -> Result<Self, String> {
        if pattern.is_empty() {
            return Err("signal name pattern cannot be empty".to_string());
        }
        let mut parts = Vec::new();
        let mut remainder = pattern;

        while let Some(open) = remainder.find('{') {
            if remainder[..open].contains('}') {
                return Err(format!("unmatched `}}` in name pattern `{pattern}`"));
            }
            if open > 0 {
                parts.push(PatternPart::Literal(Arc::from(&remainder[..open])));
            }
            let token_start = open + 1;
            let tail = &remainder[token_start..];
            let close = tail
                .find('}')
                .ok_or_else(|| format!("unclosed name-pattern token in `{pattern}`"))?;
            let token_name = &tail[..close];
            if token_name.is_empty() {
                return Err("name-pattern token cannot be empty".to_string());
            }
            let token = parse_token(token_name).ok_or_else(|| {
                format!("unknown name-pattern token `{{{token_name}}}` in `{pattern}`")
            })?;
            if is_network_token(token) && mode != DbcNamePatternMode::BusMirror {
                return Err(format!(
                    "name-pattern token `{{{token_name}}}` is only available to BusMirror schemas"
                ));
            }
            if is_bus_id_token(token) && mode == DbcNamePatternMode::BusMirror {
                return Err(format!(
                    "name-pattern token `{{{token_name}}}` is not available to BusMirror schemas; use network tokens instead"
                ));
            }
            parts.push(PatternPart::Token(token));
            remainder = &tail[close + 1..];
        }

        if remainder.contains('}') {
            return Err(format!("unmatched `}}` in name pattern `{pattern}`"));
        }
        if !remainder.is_empty() {
            parts.push(PatternPart::Literal(Arc::from(remainder)));
        }

        Ok(Self {
            parts: Arc::from(parts),
        })
    }

    /// Render a name for one DBC signal. All fallible validation happened at
    /// compile time; BusMirror-only tokens are paired with a network context.
    pub fn render(&self, context: &DbcNameContext<'_>) -> String {
        let mut output = String::new();
        for part in self.parts.iter() {
            match part {
                PatternPart::Literal(value) => output.push_str(value),
                PatternPart::Token(token) => render_token(&mut output, *token, context),
            }
        }
        output
    }
}

fn parse_token(token: &str) -> Option<NameToken> {
    Some(match token {
        "bus_name" => NameToken::BusName,
        "bus_id" => NameToken::BusIdDecimal,
        "bus_id_hex_lower" => NameToken::BusIdHexLower,
        "bus_id_hex_upper" => NameToken::BusIdHexUpper,
        "msg_id" => NameToken::MessageIdDecimal,
        "msg_id_hex_lower" => NameToken::MessageIdHexLower,
        "msg_id_hex_upper" => NameToken::MessageIdHexUpper,
        "msg_name" => NameToken::MessageName,
        "sig_name" => NameToken::SignalName,
        "network_type" => NameToken::NetworkType,
        "network_type_id" => NameToken::NetworkTypeIdDecimal,
        "network_type_id_hex_lower" => NameToken::NetworkTypeIdHexLower,
        "network_type_id_hex_upper" => NameToken::NetworkTypeIdHexUpper,
        "network_id" => NameToken::NetworkIdDecimal,
        "network_id_hex_lower" => NameToken::NetworkIdHexLower,
        "network_id_hex_upper" => NameToken::NetworkIdHexUpper,
        _ => return None,
    })
}

fn is_network_token(token: NameToken) -> bool {
    matches!(
        token,
        NameToken::NetworkType
            | NameToken::NetworkTypeIdDecimal
            | NameToken::NetworkTypeIdHexLower
            | NameToken::NetworkTypeIdHexUpper
            | NameToken::NetworkIdDecimal
            | NameToken::NetworkIdHexLower
            | NameToken::NetworkIdHexUpper
    )
}

fn is_bus_id_token(token: NameToken) -> bool {
    matches!(
        token,
        NameToken::BusIdDecimal | NameToken::BusIdHexLower | NameToken::BusIdHexUpper
    )
}

fn render_token(output: &mut String, token: NameToken, context: &DbcNameContext<'_>) {
    match token {
        NameToken::BusName => output.push_str(context.bus_name),
        NameToken::BusIdDecimal => write_number(output, format_args!("{}", context.bus_id)),
        NameToken::BusIdHexLower => write_number(output, format_args!("{:x}", context.bus_id)),
        NameToken::BusIdHexUpper => write_number(output, format_args!("{:X}", context.bus_id)),
        NameToken::MessageIdDecimal => {
            write_number(output, format_args!("{}", context.message_id));
        }
        NameToken::MessageIdHexLower => {
            write_number(output, format_args!("{:x}", context.message_id));
        }
        NameToken::MessageIdHexUpper => {
            write_number(output, format_args!("{:X}", context.message_id));
        }
        NameToken::MessageName => output.push_str(context.message_name),
        NameToken::SignalName => output.push_str(context.signal_name),
        NameToken::NetworkType => {
            output.push_str(context.network.map_or("", |network| network.network_type));
        }
        NameToken::NetworkTypeIdDecimal => write_number(
            output,
            format_args!(
                "{}",
                context.network.map_or(0, |network| network.network_type_id)
            ),
        ),
        NameToken::NetworkTypeIdHexLower => write_number(
            output,
            format_args!(
                "{:x}",
                context.network.map_or(0, |network| network.network_type_id)
            ),
        ),
        NameToken::NetworkTypeIdHexUpper => write_number(
            output,
            format_args!(
                "{:X}",
                context.network.map_or(0, |network| network.network_type_id)
            ),
        ),
        NameToken::NetworkIdDecimal => write_number(
            output,
            format_args!(
                "{}",
                context.network.map_or(0, |network| network.network_id)
            ),
        ),
        NameToken::NetworkIdHexLower => write_number(
            output,
            format_args!(
                "{:x}",
                context.network.map_or(0, |network| network.network_id)
            ),
        ),
        NameToken::NetworkIdHexUpper => write_number(
            output,
            format_args!(
                "{:X}",
                context.network.map_or(0, |network| network.network_id)
            ),
        ),
    }
}

fn write_number(output: &mut String, args: std::fmt::Arguments<'_>) {
    let _ = output.write_fmt(args);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_relation_names_and_explicit_hex_case() {
        let pattern = CompiledNamePattern::compile(
            "{bus_name}_{bus_id_hex_upper}_{msg_id}_{msg_id_hex_lower}_{msg_id_hex_upper}_{msg_name}_{sig_name}",
            DbcNamePatternMode::Standard,
        )
        .expect("compile pattern");
        let name = pattern.render(&DbcNameContext {
            bus_name: "Powertrain",
            bus_id: 0x101,
            message_id: 0x2ab,
            message_name: "VehicleStatus",
            signal_name: "Speed",
            network: None,
        });
        assert_eq!(name, "Powertrain_101_683_2ab_2AB_VehicleStatus_Speed");

        let pattern = CompiledNamePattern::compile(
            "{network_type}{network_id}_{network_type_id_hex_upper}_{network_id_hex_upper}_{msg_name}_{sig_name}",
            DbcNamePatternMode::BusMirror,
        )
        .expect("compile BusMirror pattern");
        let name = pattern.render(&DbcNameContext {
            bus_name: "Powertrain",
            bus_id: 0x101,
            message_id: 0x2ab,
            message_name: "VehicleStatus",
            signal_name: "Speed",
            network: Some(NetworkNameContext {
                network_type: "can",
                network_type_id: 1,
                network_id: 10,
            }),
        });
        assert_eq!(name, "can10_1_A_VehicleStatus_Speed");
    }

    #[test]
    fn rejects_unknown_unclosed_and_out_of_context_tokens() {
        assert!(CompiledNamePattern::compile("{id}", DbcNamePatternMode::BusMirror).is_err());
        assert!(CompiledNamePattern::compile("{msg_name", DbcNamePatternMode::BusMirror).is_err());
        assert!(
            CompiledNamePattern::compile("{network_id}", DbcNamePatternMode::Standard).is_err()
        );
        assert!(CompiledNamePattern::compile("{bus_id}", DbcNamePatternMode::BusMirror).is_err());
    }
}
