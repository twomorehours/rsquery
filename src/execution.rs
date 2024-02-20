use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::{
    data_source::MemoryDataSource,
    logical_plan::{self, DataFrame},
};

pub struct ExecutionContext {}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {}
    }

    pub fn memory(&self, batch: RecordBatch) -> DataFrame {
        let projection = batch
            .schema()
            .all_fields()
            .iter()
            .map(|c| c.name())
            .cloned()
            .collect::<Vec<_>>();
        DataFrame(Arc::new(logical_plan::Scan::new(
            "".to_owned(),
            Arc::new(MemoryDataSource::new(batch)),
            projection,
        )))
    }
}
