#![allow(dead_code)]
#[cfg(test)]
mod tests {
    use apache_avro::schema::derive::AvroSchemaComponent;
    use apache_avro::{
        AvroResult, AvroSchema, Writer,
        error::Details,
        schema::Schema,
        to_value,
        types::{Record, Value},
    };
    use apache_avro_macros::serdavro;
    use serde::Serialize;
    use std::io::Write;

    pub trait SerdavroWriter {
        fn append_serdavro<T>(&mut self, value: &T) -> AvroResult<usize>
        where
            T: AvroSchema + Serialize;
    }

    fn append_serdavro_recursive<T>(value: &T, schema: &Schema) -> AvroResult<Value>
    where
        T: AvroSchema + Serialize,
    {
        let value = to_value(value)?;

        // recursive helper that walks a Value according to a Schema and
        // converts nested maps/records/arrays/maps into the proper Avro
        // Value forms using the provided schema information.
        fn recurse(val: Value, schema: &Schema) -> AvroResult<Value> {
            use apache_avro::schema::Schema as AvroSchemaEnum;

            match schema {
                AvroSchemaEnum::Record(record_schema) => {
                    let mut record =
                        Record::new(schema).ok_or_else(|| Details::ValidationWithReason {
                            value: val.clone(),
                            schema: schema.clone(),
                            reason: "expected a record schema".to_string(),
                        })?;

                    match val {
                        Value::Map(m) => {
                            for (key, value) in m.into_iter() {
                                // find corresponding field schema if present
                                let processed = if let Some(field) =
                                    record_schema.fields.iter().find(|f| f.name == key)
                                {
                                    recurse(value, &field.schema)?
                                } else {
                                    value
                                };
                                record.put(&key, processed);
                            }
                            Ok(Value::Record(record.fields))
                        }
                        Value::Record(rvec) => {
                            for (key, value) in rvec.into_iter() {
                                let processed = if let Some(field) =
                                    record_schema.fields.iter().find(|f| f.name == key)
                                {
                                    recurse(value, &field.schema)?
                                } else {
                                    value
                                };
                                record.put(&key, processed);
                            }
                            Ok(Value::Record(record.fields))
                        }
                        other => Err(Details::ValidationWithReason {
                            value: other,
                            schema: schema.clone(),
                            reason: "expected a map or record".to_string(),
                        }
                        .into()),
                    }
                }
                // For arrays and maps we don't need special handling for this
                // test case (no nested arrays/maps). Return the value as-is
                // so compilation doesn't depend on apache-avro internals.
                AvroSchemaEnum::Array(_) => Ok(val),
                AvroSchemaEnum::Map(_) => Ok(val),
                // For primitives and other schema kinds, return the value as-is
                _ => Ok(val),
            }
        }

        recurse(value, schema)
    }

    impl<W> SerdavroWriter for Writer<'_, W>
    where
        W: Write,
    {
        fn append_serdavro<T>(&mut self, value: &T) -> AvroResult<usize>
        where
            T: AvroSchema + Serialize,
        {
            let schema = self.schema();
            let avro_value = append_serdavro_recursive(value, &schema)?;
            self.append(avro_value)
        }
    }

    #[test]
    fn avro_schema_is_flattened_with_rename() {
        #[serdavro]
        #[derive(Debug, Serialize, PartialEq)]
        struct Nested {
            #[serde(rename = "flat_name")]
            pub a: i32,
            pub b: String,
        }

        #[serdavro]
        #[derive(Debug, Serialize, PartialEq)]
        struct S {
            #[serde(flatten)]
            pub nested: Nested,
            pub other: i32,
        }

        let schema = S::get_schema_in_ctxt(&mut std::collections::HashMap::new(), &None);
        if let Schema::Record(record_schema) = schema.clone() {
            let field_names: Vec<String> = record_schema
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect();
            assert!(!field_names.contains(&"a".to_string()));
            assert!(field_names.contains(&"b".to_string()));
            assert!(field_names.contains(&"other".to_string()));
            assert!(!field_names.contains(&"nested".to_string()));
            assert!(field_names.contains(&"flat_name".to_string()));
        } else {
            panic!("Expected record schema");
        }

        let s_instance = S {
            nested: Nested {
                a: 42,
                b: "hello".to_string(),
            },
            other: 7,
        };

        let mut writer = Writer::with_codec(&schema, Vec::new(), apache_avro::Codec::Null);
        writer
            .append_serdavro(&s_instance)
            .expect("Failed to append serdavro");
        let _ = writer.into_inner().expect("Failed to get inner writer");
    }

    #[test]
    fn avro_schema_enum_generation() {
        #[serdavro]
        #[derive(Debug, Serialize, PartialEq)]
        enum MyEnum {
            #[serde(rename = "a")]
            A,
            #[serde(rename = "b")]
            B,
            #[serde(rename = "c")]
            C,
        }

        let schema = MyEnum::get_schema_in_ctxt(&mut std::collections::HashMap::new(), &None);
        if let Schema::Enum(enum_schema) = schema.clone() {
            let symbols = &enum_schema.symbols;
            assert!(symbols.contains(&"a".to_string()));
            assert!(symbols.contains(&"b".to_string()));
            assert!(symbols.contains(&"c".to_string()));
        } else {
            panic!("Expected enum schema");
        }

        let enum_instance = MyEnum::B;
        let mut writer = Writer::with_codec(&schema, Vec::new(), apache_avro::Codec::Null);
        writer
            .append_serdavro(&enum_instance)
            .expect("Failed to append serdavro");
        let _ = writer.into_inner().expect("Failed to get inner writer");
    }
}
