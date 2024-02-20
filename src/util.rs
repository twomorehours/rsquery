use arrow::datatypes::{Schema, SchemaRef};

pub fn schema_select(schema: SchemaRef, projection: &[String]) -> Schema {
    let project_fileds: Vec<_> = schema
        .all_fields()
        .into_iter()
        .filter(|f| projection.contains(f.name()))
        .cloned()
        .collect();
    Schema::new_with_metadata(project_fileds, schema.metadata().clone())
}
