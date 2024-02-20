use std::sync::Arc;

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::schema_select;

pub trait DataSource {
    fn schema(&self) -> SchemaRef;
    fn scan(&self, projection: &[String]) -> Box<dyn Iterator<Item = RecordBatch>>;
}

pub struct MemoryDataSource {
    batch: RecordBatch,
}

impl MemoryDataSource {
    pub fn new(batch: RecordBatch) -> MemoryDataSource {
        Self { batch }
    }
}

impl DataSource for MemoryDataSource {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn scan(&self, projection: &[String]) -> Box<dyn Iterator<Item = RecordBatch>> {
        let new_schema = schema_select(self.batch.schema(), projection);
        let new_columns: Vec<_> = new_schema
            .all_fields()
            .iter()
            .map(|f| self.batch.column_by_name(f.name()).unwrap())
            .cloned()
            .collect();

        let new_batch = RecordBatch::try_new(Arc::new(new_schema), new_columns).unwrap();

        Box::new(vec![new_batch].into_iter())
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field},
    };

    use super::*;

    #[test]
    fn test_scan() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let column1 = Int64Array::from(vec![1, 2, 3]);
        let column2 = StringArray::from(vec!["a", "b", "c"]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(column1), Arc::new(column2.clone())],
        )
        .unwrap();

        let data_source = MemoryDataSource::new(batch);

        let projection = vec!["name".to_string()];
        let result = data_source.scan(&projection);

        let expected_schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let expected_batch =
            RecordBatch::try_new(Arc::new(expected_schema), vec![Arc::new(column2)]).unwrap();

        let result_batches = result.collect::<Vec<_>>();

        assert_eq!(result_batches.len(), 1);

        assert_eq!(&result_batches[0], &expected_batch);
    }
}
