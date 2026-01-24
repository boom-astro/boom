use apache_avro::{AvroSchema, Schema};

pub fn get_avro_schema<T: AvroSchema>() -> Schema {
    let schema = T::get_schema();
    // Looks like the generated schema may have repetitions of named types
    // so we get the canonical form to deduplicate them, then load them back
    let canonical = schema.canonical_form();
    Schema::parse_str(&canonical).expect("Failed to parse Avro schema")
}
