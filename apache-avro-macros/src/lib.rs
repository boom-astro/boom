use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Fields, Item, ItemEnum, ItemStruct, parse_macro_input, spanned::Spanned};

#[proc_macro_attribute]
pub fn serdavro(
    _: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let parsed = parse_macro_input!(item as Item);

    match &parsed {
        Item::Struct(s) => serdavro_impl(s)
            .unwrap_or_else(syn::Error::into_compile_error)
            .into(),
        Item::Enum(e) => serdavro_enum_impl(e)
            .unwrap_or_else(syn::Error::into_compile_error)
            .into(),
        _ => syn::Error::new(
            parsed.span(),
            "`#[serdavro]` can only be applied to structs or enums",
        )
        .into_compile_error()
        .into(),
    }
}

fn serdavro_impl(item: &ItemStruct) -> syn::Result<TokenStream> {
    let ident = &item.ident;

    let fake_ident = format_ident!("__SERDAVRO__{ident}__");
    let mut fake = item.clone();
    fake.ident = fake_ident.clone();

    // Collect flattened field names (include rename variants for flattened fields)
    let flatten_fields = collect_flatten_field_names(item)?;
    // Collect rename pairs for fields that have `#[serde(rename = "...")]` so we
    // can apply those renames in the generated Avro schema.
    let rename_pairs = collect_field_renames(item)?;
    let rename_pairs_tokens: Vec<TokenStream> = rename_pairs
        .iter()
        .map(|(from, to)| {
            let f = proc_macro2::Literal::string(from);
            let t = proc_macro2::Literal::string(to);
            quote!((#f, #t))
        })
        .collect();

    // Turn flatten field name strings into tokenized string literals so they
    // can be interpolated into `quote!` repetitions.
    let flatten_fields_tokens: Vec<TokenStream> = flatten_fields
        .iter()
        .map(|s| {
            let lit = proc_macro2::Literal::string(s);
            quote!(#lit)
        })
        .collect();

    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    Ok(quote! {
        const _: () = {
            use ::std::collections::HashMap;

            use ::apache_avro::{
                AvroSchema,
                schema::{Name, RecordField, RecordSchema, Schema, derive::AvroSchemaComponent},
            };

            #[derive(AvroSchema)]
            #fake

            #[automatically_derived]
            impl #impl_generics AvroSchemaComponent for #ident #ty_generics #where_clause {
                fn get_schema_in_ctxt(named_schemas: &mut HashMap<Name, Schema>, enclosing_namespace: &Option<String>) -> Schema {
                    let mut fake_schema = #fake_ident::get_schema_in_ctxt(named_schemas, enclosing_namespace);
                    match fake_schema {
                        Schema::Record(fake) => {
                            let mut record = fake.clone();
                            record.name = Name::new(stringify!(#ident)).expect("Unable to parse schema name");
                            record.lookup.clear();
                            record.fields.clear();

                            fn add(record: &mut RecordSchema, mut field: RecordField, index: &mut usize) {
                                record.lookup.insert(field.name.clone(), *index);
                                field.position = *index;
                                record.fields.push(field);
                                *index += 1;
                            };

                            let mut index = 0;

                            // Rename pairs emitted as a slice of (from, to) string pairs.
                            let renames: &[(&str, &str)] = &[#(#rename_pairs_tokens),*];

                            for mut field in fake.fields {
                                // Apply rename if the field currently matches the original name.
                                for (from, to) in renames {
                                    if field.name == *from {
                                        field.name = to.to_string();
                                        break;
                                    }
                                }

                                // Flatten names as a slice of `&str` and check membership by
                                // comparing the `String` `field.name` against each `&str`.
                                let flatten_names: &[&str] = &[#(#flatten_fields_tokens),*];

                                if flatten_names.iter().any(|n| field.name == *n) {
                                    match field.schema {
                                        Schema::Record(r) => {
                                            for field in r.fields {
                                                add(&mut record, field, &mut index);
                                            }
                                        }
                                        _ => panic!("Only record schemas can be flattened")
                                    }
                                } else {
                                    add(&mut record, field, &mut index);
                                }
                            }

                            Schema::Record(record)
                        }
                        _ => panic!("Only record schemas are supported")
                    }
                }
            }
        };

        #item
    })
}

// the above didn't actually rename the enum variants, so we need to do it manually
fn serdavro_enum_impl(item: &syn::ItemEnum) -> syn::Result<TokenStream> {
    let ident = &item.ident;

    let fake_ident = format_ident!("__SERDAVRO__{ident}__");
    let mut fake = item.clone();
    fake.ident = fake_ident.clone();

    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    // Collect rename pairs for enum variants (#[serde(rename = "...")])
    let variant_renames = collect_variant_renames(item)?;
    let variant_renames_tokens: Vec<TokenStream> = variant_renames
        .iter()
        .map(|(from, to)| {
            let f = proc_macro2::Literal::string(from);
            let t = proc_macro2::Literal::string(to);
            quote!((#f, #t))
        })
        .collect();

    Ok(quote! {
        const _: () = {
            use ::std::collections::HashMap;

            use ::apache_avro::{
                AvroSchema,
                schema::{Name, Schema, derive::AvroSchemaComponent},
            };

            #[derive(AvroSchema)]
            #fake

            #[automatically_derived]
            impl #impl_generics AvroSchemaComponent for #ident #ty_generics #where_clause {
                fn get_schema_in_ctxt(named_schemas: &mut HashMap<Name, Schema>, enclosing_namespace: &Option<String>) -> Schema {
                    let mut fake_schema = #fake_ident::get_schema_in_ctxt(named_schemas, enclosing_namespace);
                    match fake_schema {
                        Schema::Enum(mut enum_schema) => {
                            enum_schema.name = Name::new(stringify!(#ident)).expect("Unable to parse schema name");

                            // Apply renames to enum symbols
                            let renames: &[(&str, &str)] = &[#(#variant_renames_tokens),*];

                            for symbol in enum_schema.symbols.iter_mut() {
                                for (from, to) in renames {
                                    if symbol == from {
                                        *symbol = to.to_string();
                                        break;
                                    }
                                }
                            }

                            Schema::Enum(enum_schema)
                        }
                        Schema::Ref { name } => {
                            // Schema::Ref { name  }
                            // return a Schema ref but remove the __SERDAVRO__ prefix and trailing __, which otherwise
                            // makes it an invalid reference
                            let name_str = name.to_string();
                            let clean_name_str = name_str.trim_start_matches("__SERDAVRO__").trim_end_matches("__");
                            let clean_name = Name::new(clean_name_str).expect("Unable to parse schema name");
                            Schema::Ref { name: clean_name }

                        }
                        _ => panic!("Only enum schemas are supported")
                    }
                }
            }
        };

        #item
    })
}

// Helper to collect flattened field names from an ItemStruct.
// For each field annotated with `#[serde(flatten)]` this returns a list
// containing the original identifier and, if present, the `rename` value.
fn collect_flatten_field_names(item: &ItemStruct) -> syn::Result<Vec<String>> {
    match &item.fields {
        Fields::Named(fields) => {
            let mut names = Vec::new();
            for field in fields.named.iter() {
                let orig = field.ident.clone().unwrap().to_string();
                let mut is_flatten = false;
                let mut rename: Option<String> = None;

                for attr in &field.attrs {
                    if attr.path().is_ident("serde") {
                        let _ = attr.parse_nested_meta(|meta| {
                            if meta.path.is_ident("flatten") {
                                is_flatten = true;
                            } else if meta.path.is_ident("rename") {
                                if let Ok(pbuf) = meta.value() {
                                    if let Ok(lit) = pbuf.parse::<syn::Lit>() {
                                        if let syn::Lit::Str(s) = lit {
                                            rename = Some(s.value());
                                        }
                                    }
                                }
                            }
                            Ok(())
                        });
                    }
                }

                if is_flatten {
                    names.push(orig.clone());
                    if let Some(r) = rename {
                        names.push(r);
                    }
                }
            }
            Ok(names)
        }
        _ => Err(syn::Error::new(
            item.fields.span(),
            "Only named fields are supported",
        )),
    }
}

// Collect (orig, rename) pairs for fields that have `#[serde(rename = "...")]`.
fn collect_field_renames(item: &ItemStruct) -> syn::Result<Vec<(String, String)>> {
    match &item.fields {
        Fields::Named(fields) => {
            let mut pairs = Vec::new();
            for field in fields.named.iter() {
                let orig = field.ident.clone().unwrap().to_string();
                for attr in &field.attrs {
                    if attr.path().is_ident("serde") {
                        let mut rename: Option<String> = None;
                        let _ = attr.parse_nested_meta(|meta| {
                            if meta.path.is_ident("rename") {
                                if let Ok(pbuf) = meta.value() {
                                    if let Ok(lit) = pbuf.parse::<syn::Lit>() {
                                        if let syn::Lit::Str(s) = lit {
                                            rename = Some(s.value());
                                        }
                                    }
                                }
                            }
                            Ok(())
                        });

                        if let Some(r) = rename {
                            pairs.push((orig.clone(), r));
                        }
                    }
                }
            }
            Ok(pairs)
        }
        _ => Err(syn::Error::new(
            item.fields.span(),
            "Only named fields are supported",
        )),
    }
}

// Collect (orig, rename) pairs for enum variants that have `#[serde(rename = "...")]`.
fn collect_variant_renames(item: &ItemEnum) -> syn::Result<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    for variant in item.variants.iter() {
        let orig = variant.ident.clone().to_string();
        for attr in &variant.attrs {
            if attr.path().is_ident("serde") {
                let mut rename: Option<String> = None;
                let _ = attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename") {
                        if let Ok(pbuf) = meta.value() {
                            if let Ok(lit) = pbuf.parse::<syn::Lit>() {
                                if let syn::Lit::Str(s) = lit {
                                    rename = Some(s.value());
                                }
                            }
                        }
                    }
                    Ok(())
                });

                if let Some(r) = rename {
                    pairs.push((orig.clone(), r));
                }
            }
        }
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_with_rename_collects_both() {
        let s: ItemStruct = syn::parse_str(
            r#"
            struct S {
                #[serde(flatten, rename = "flat_name")]
                nested: Nested,
                other: i32,
            }
        "#,
        )
        .unwrap();

        let names = collect_flatten_field_names(&s).unwrap();
        assert!(names.contains(&"nested".to_string()));
        assert!(names.contains(&"flat_name".to_string()));
    }

    #[test]
    fn rename_without_flatten_not_collected() {
        let s: ItemStruct = syn::parse_str(
            r#"
            struct S {
                #[serde(rename = "renamed")]
                field: i32,
            }
        "#,
        )
        .unwrap();

        let names = collect_flatten_field_names(&s).unwrap();
        assert!(!names.contains(&"field".to_string()));
        assert!(!names.contains(&"renamed".to_string()));
    }

    #[test]
    fn field_rename_collected() {
        let s: ItemStruct = syn::parse_str(
            r#"
            struct S {
                #[serde(rename = "renamed")]
                field: i32,
                other: String,
            }
        "#,
        )
        .unwrap();

        let pairs = collect_field_renames(&s).unwrap();
        assert!(pairs.contains(&("field".to_string(), "renamed".to_string())));
    }
}
