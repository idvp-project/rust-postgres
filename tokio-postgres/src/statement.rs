use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::Type;
use postgres_protocol::message::frontend;
use std::{
    fmt,
    sync::{Arc, Weak},
};

struct StatementInner {
    client: Weak<InnerClient>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let buf = client.with_buf(|buf| {
                frontend::close(b'S', &self.name, buf).unwrap();
                frontend::sync(buf);
                buf.split().freeze()
            });
            let _ = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)));
        }
    }
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(
        inner: &Arc<InnerClient>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement(Arc::new(StatementInner {
            client: Arc::downgrade(inner),
            name,
            params,
            columns,
        }))
    }

    pub(crate) fn name(&self) -> &str {
        &self.0.name
    }

    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Type] {
        &self.0.params
    }

    /// Returns information about the columns returned when the statement is queried.
    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}

/// Information about a column of a query.
pub struct Column {
    name: String,
    type_: Type,
    type_modifier: i32,
}

impl Column {
    pub(crate) fn new(name: String, type_: Type, type_modifier: i32) -> Column {
        Column { name, type_, type_modifier }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn type_(&self) -> &Type {
        &self.type_
    }

    /// Returns precision of the column.
    #[allow(overflowing_literals)]
    pub fn precision(&self) -> Option<u32> {
        match self.type_ {
            Type::INT2 => Some(5), // -32768 to +32767

            Type::OID => Some(10), // -2147483648 to +2147483647
            Type::INT4 => Some(10), // 0 to 4294967295

            Type::INT8 => Some(19), // -9223372036854775808 to +9223372036854775807

            // For float4 and float8, we can normally only get 6 and 15
            // significant digits out, but extra_float_digits may raise
            // that number by up to two digits.
            Type::FLOAT4 => Some(8), // sign + 9 digits + decimal point + e + sign + 2 digits

            Type::FLOAT8 => Some(17), // sign + 18 digits + decimal point + e + sign + 3 digits

            Type::NUMERIC => {
                if self.type_modifier == -1 {
                    None
                } else {
                    Some((((self.type_modifier - 4) & 0xFFFF0000) >> 16) as u32)
                }
            }

            Type::CHAR => Some(1),
            Type::BOOL => Some(1),

            Type::BPCHAR | Type::VARCHAR => {
                if self.type_modifier == -1 {
                    None
                } else {
                    Some((self.type_modifier - 4) as u32)
                }
            }

            Type::DATE => Some(13),  // "4713-01-01 BC" to "01/01/4713 BC" - "31/12/32767"

            // We assume the worst case scenario for all of these.
            // time = '00:00:00' = 8
            // date = '5874897-12-31' = 13 (although at large values second precision is lost)
            // date = '294276-11-20' = 12 --enable-integer-datetimes
            // zone = '+11:30' = 6;
            Type::TIME => {
                let second_size = Column::type_modifier_to_second_size(self.type_modifier);
                Some((8 + second_size) as u32)
            }
            Type::TIMETZ => {
                let second_size = Column::type_modifier_to_second_size(self.type_modifier);
                Some((8 + second_size + 6) as u32)
            }
            Type::TIMESTAMP => {
                let second_size = Column::type_modifier_to_second_size(self.type_modifier);
                Some((13 + 1 + 8 + second_size) as u32)
            }
            Type::TIMESTAMPTZ => {
                let second_size = Column::type_modifier_to_second_size(self.type_modifier);
                Some((13 + 1 + 8 + second_size + 4) as u32)
            }
            Type::INTERVAL => Some(49),
            Type::BIT => Some(self.type_modifier as u32),
            Type::VARBIT => {
                if self.type_modifier == -1 {
                    None
                } else {
                    Some(self.type_modifier as u32)
                }
            }
            _ => None
        }
    }

    /// Returns scale of the column.
    pub fn scale(&self) -> Option<u32> {
        match self.type_ {
            Type::FLOAT4 => Some(8),
            Type::FLOAT8 => Some(17),
            Type::NUMERIC => {
                if self.type_modifier == -1 {
                    Some(0)
                } else {
                    Some(((self.type_modifier - 4) & 0xFFFF) as u32)
                }
            }
            Type::TIME | Type::TIMETZ | Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                if self.type_modifier == -1 {
                    Some(6)
                } else {
                    Some(self.type_modifier as u32)
                }
            }
            Type::INTERVAL => {
                if self.type_modifier == -1 {
                    Some(6)
                } else {
                    Some((self.type_modifier & 0xFFFF) as u32)
                }
            }
            _ => None
        }
    }

    #[inline]
    fn type_modifier_to_second_size(type_modifier: i32) -> i32 {
        match type_modifier {
            -1 => 7,
            0 => 0,
            1 => 3,
            x => x + 1
        }
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Column")
            .field("name", &self.name)
            .field("type", &self.type_)
            .finish()
    }
}
