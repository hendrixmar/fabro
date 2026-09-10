use serde_json::Value;

/// The reason a parsed JSON value cannot be represented as a TOML scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum JsonScalarToTomlError {
    /// JSON null has no TOML scalar representation.
    #[error("JSON null is not a TOML scalar")]
    Null,
    /// JSON arrays are outside the scalar-only conversion contract.
    #[error("JSON arrays are not TOML scalars")]
    Array,
    /// JSON objects are outside the scalar-only conversion contract.
    #[error("JSON objects are not TOML scalars")]
    Object,
    /// The JSON number is not representable by the supported TOML number types.
    #[error("JSON number is outside TOML's supported range")]
    NumberOutOfRange,
}

/// The reason a parsed TOML value cannot be represented as a JSON scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TomlScalarToJsonError {
    /// The TOML float is not finite, and JSON numbers must be finite.
    #[error("must be a finite float")]
    NonFiniteFloat,
    /// TOML datetimes are outside the scalar-only conversion contract.
    #[error("must be a scalar value")]
    Datetime,
    /// TOML arrays are outside the scalar-only conversion contract.
    #[error("must be a scalar value")]
    Array,
    /// TOML tables are outside the scalar-only conversion contract.
    #[error("must be a scalar value")]
    Table,
}

/// Converts an already-parsed TOML scalar into a JSON value.
///
/// The inverse of [`json_scalar_to_toml_value`]: strings, booleans, and
/// integers map directly, and finite floats become JSON numbers.
///
/// # Errors
///
/// Returns [`TomlScalarToJsonError`] for TOML datetimes, arrays, tables, or a
/// non-finite float (JSON numbers must be finite).
pub fn toml_scalar_to_json_value(value: &toml::Value) -> Result<Value, TomlScalarToJsonError> {
    match value {
        toml::Value::String(value) => Ok(Value::String(value.clone())),
        toml::Value::Integer(value) => Ok(Value::Number((*value).into())),
        toml::Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .ok_or(TomlScalarToJsonError::NonFiniteFloat),
        toml::Value::Boolean(value) => Ok(Value::Bool(*value)),
        toml::Value::Datetime(_) => Err(TomlScalarToJsonError::Datetime),
        toml::Value::Array(_) => Err(TomlScalarToJsonError::Array),
        toml::Value::Table(_) => Err(TomlScalarToJsonError::Table),
    }
}

/// Converts an already-parsed JSON scalar into a TOML value.
///
/// Numbers are converted to a signed integer first and then to a float. As a
/// result, nonnegative integers above `i64::MAX` become TOML floats and may
/// lose integer precision. JSON floats remain TOML floats.
///
/// # Errors
///
/// Returns [`JsonScalarToTomlError`] for JSON null, arrays, objects, or a
/// number that can be represented as neither an `i64` nor an `f64`.
pub fn json_scalar_to_toml_value(value: &Value) -> Result<toml::Value, JsonScalarToTomlError> {
    match value {
        Value::Null => Err(JsonScalarToTomlError::Null),
        Value::Bool(value) => Ok(toml::Value::Boolean(*value)),
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Ok(toml::Value::Integer(integer))
            } else if let Some(float) = value.as_f64() {
                Ok(toml::Value::Float(float))
            } else {
                Err(JsonScalarToTomlError::NumberOutOfRange)
            }
        }
        Value::String(value) => Ok(toml::Value::String(value.clone())),
        Value::Array(_) => Err(JsonScalarToTomlError::Array),
        Value::Object(_) => Err(JsonScalarToTomlError::Object),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        JsonScalarToTomlError, TomlScalarToJsonError, json_scalar_to_toml_value,
        toml_scalar_to_json_value,
    };

    #[test]
    fn converts_toml_scalars_to_json_values() -> Result<(), TomlScalarToJsonError> {
        let cases = [
            (toml::Value::String("hello".to_string()), json!("hello")),
            (toml::Value::Integer(42), json!(42)),
            (toml::Value::Float(1.25), json!(1.25)),
            (toml::Value::Boolean(true), json!(true)),
        ];

        for (input, expected) in cases {
            assert_eq!(toml_scalar_to_json_value(&input)?, expected);
        }

        Ok(())
    }

    #[test]
    fn rejects_non_scalar_toml_values_with_typed_errors() {
        let datetime: toml::Value = "value = 1979-05-27T07:32:00Z"
            .parse::<toml::Table>()
            .unwrap()
            .remove("value")
            .unwrap();
        let cases = [
            (datetime, TomlScalarToJsonError::Datetime),
            (
                toml::Value::Array(vec![toml::Value::Integer(1)]),
                TomlScalarToJsonError::Array,
            ),
            (
                toml::Value::Table(toml::Table::new()),
                TomlScalarToJsonError::Table,
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(toml_scalar_to_json_value(&input), Err(expected));
        }
    }

    #[test]
    fn rejects_non_finite_toml_floats() {
        for input in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                toml_scalar_to_json_value(&toml::Value::Float(input)),
                Err(TomlScalarToJsonError::NonFiniteFloat)
            );
        }
    }

    #[test]
    fn toml_scalars_round_trip_through_json() -> Result<(), TomlScalarToJsonError> {
        let scalars = [
            toml::Value::String("hello".to_string()),
            toml::Value::Integer(i64::MIN),
            toml::Value::Integer(i64::MAX),
            toml::Value::Float(1.25),
            toml::Value::Boolean(false),
        ];

        for input in scalars {
            let json = toml_scalar_to_json_value(&input)?;
            assert_eq!(
                json_scalar_to_toml_value(&json).expect("round trip should stay scalar"),
                input
            );
        }

        Ok(())
    }

    #[test]
    fn converts_strings_to_toml_strings() -> Result<(), JsonScalarToTomlError> {
        for input in ["", "hello", "Grüße 世界"] {
            let json = Value::String(input.to_string());

            assert_eq!(
                json_scalar_to_toml_value(&json)?,
                toml::Value::String(input.to_string()),
                "{input:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn converts_booleans_to_toml_booleans() -> Result<(), JsonScalarToTomlError> {
        for input in [false, true] {
            assert_eq!(
                json_scalar_to_toml_value(&Value::Bool(input))?,
                toml::Value::Boolean(input)
            );
        }

        Ok(())
    }

    #[test]
    fn converts_signed_range_integers_to_toml_integers() -> Result<(), JsonScalarToTomlError> {
        for input in [i64::MIN, -1, 0, i64::MAX] {
            assert_eq!(
                json_scalar_to_toml_value(&Value::from(input))?,
                toml::Value::Integer(input)
            );
        }

        let signed_max_as_u64 =
            u64::try_from(i64::MAX).expect("i64::MAX should be representable as u64");
        assert_eq!(
            json_scalar_to_toml_value(&Value::from(signed_max_as_u64))?,
            toml::Value::Integer(i64::MAX)
        );

        Ok(())
    }

    #[test]
    fn converts_large_unsigned_integers_to_toml_floats() -> Result<(), JsonScalarToTomlError> {
        let signed_max_as_u64 =
            u64::try_from(i64::MAX).expect("i64::MAX should be representable as u64");
        for input in [signed_max_as_u64 + 1, u64::MAX] {
            assert_eq!(
                json_scalar_to_toml_value(&Value::from(input))?,
                toml::Value::Float(input as f64)
            );
        }

        Ok(())
    }

    #[test]
    fn preserves_finite_json_floats_as_toml_floats() -> Result<(), JsonScalarToTomlError> {
        for input in [-0.0, 0.5, 1.0, f64::MAX] {
            let converted = json_scalar_to_toml_value(&Value::from(input))?;
            let toml::Value::Float(output) = converted else {
                panic!("{input:?} should remain a TOML float");
            };

            assert_eq!(output.to_bits(), input.to_bits(), "{input:?}");
        }

        Ok(())
    }

    #[test]
    fn rejects_non_scalar_json_values_with_typed_errors() {
        let cases = [
            (Value::Null, JsonScalarToTomlError::Null),
            (json!(["value"]), JsonScalarToTomlError::Array),
            (json!({ "key": "value" }), JsonScalarToTomlError::Object),
        ];

        for (input, expected) in cases {
            assert_eq!(json_scalar_to_toml_value(&input), Err(expected));
        }
    }

    #[test]
    fn classifies_non_finite_float_values_at_the_json_boundary() {
        for input in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let json = Value::from(input);

            assert_eq!(json, Value::Null);
            assert_eq!(
                json_scalar_to_toml_value(&json),
                Err(JsonScalarToTomlError::Null)
            );
        }
    }
}
