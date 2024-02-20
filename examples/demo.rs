use std::{rc::Rc, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    util::pretty::print_batches,
};
use rsquery::{execution::ExecutionContext, logical_plan, query_planner::create_physical_plan};

fn main() {
    // select id,name from users where name == "yuhao" or id > 1 limit 10;
    let ctx = ExecutionContext::new();
    let or = logical_plan::Or::new(
        Rc::new(logical_plan::Eq::new(
            Rc::new(logical_plan::col("name".to_string())),
            Rc::new(logical_plan::string_lit("yuhao".to_string())),
        )),
        Rc::new(logical_plan::Gt::new(
            Rc::new(logical_plan::Add::new(
                Rc::new(logical_plan::col("id".to_string())),
                Rc::new(logical_plan::long_lit(1)),
            )),
            Rc::new(logical_plan::long_lit(3)),
        )),
    );
    let df = ctx
        .memory(create_record_batch())
        .filter(Rc::new(or))
        .aggregate(
            vec![Rc::new(logical_plan::Column {
                name: "name".to_owned(),
            })],
            vec![Rc::new(logical_plan::Min {
                expr: Rc::new(logical_plan::Column {
                    name: "id".to_owned(),
                }),
                name: "min_id".to_string(),
            })],
        )
        .project(vec![
            Rc::new(logical_plan::col("name".to_owned())),
            Rc::new(logical_plan::col("min_id".to_owned())),
        ]);

    let pp = create_physical_plan(df.logic_plan());

    for batch in pp.execute() {
        print_batches(&[batch]).unwrap();
    }
}

fn create_record_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let ids: Vec<_> = (0..100).map(|i| i as i64).collect();
    let column1 = Int64Array::from(ids);
    let names: Vec<_> = (0..100).map(|i| format!("name-{}", i % 10)).collect();
    let column2 = StringArray::from(names);
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(column1), Arc::new(column2.clone())],
    )
    .unwrap()
}
