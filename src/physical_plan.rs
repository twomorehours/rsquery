use std::{cell::RefCell, collections::HashMap, fmt::Display, rc::Rc, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, BooleanArray, Datum, Int64Array, Int64Builder, StringArray,
        StringBuilder,
    },
    compute::kernels,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};

use crate::{data_source::DataSource, schema_select};

pub enum ArrayOrDatum {
    Array(Arc<dyn Array>),
    Datum(Arc<dyn Datum>),
}

impl ArrayOrDatum {
    pub fn get_datum_ref(&self) -> &dyn Datum {
        match self {
            ArrayOrDatum::Array(ref arr) => arr,
            ArrayOrDatum::Datum(datum) => datum.as_ref(),
        }
    }
}

pub trait PhysicalPlan: Display {
    fn schema(&self) -> SchemaRef;
    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_>;
    fn children(&self) -> Vec<Rc<dyn PhysicalPlan>>;
}

pub trait PhysicalExpr: Display {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum;
}

pub fn pretty_print_plan(plan: Rc<dyn PhysicalPlan>) -> String {
    _pretty_print_plan(plan, 0)
}
fn _pretty_print_plan(plan: Rc<dyn PhysicalPlan>, indent: u32) -> String {
    let mut s = String::new();

    for _ in 0..indent {
        s.push('\t');
    }

    s.push_str(&plan.to_string());
    s.push('\n');

    for child in plan.children() {
        s.push_str(&_pretty_print_plan(child, indent + 1));
    }
    s
}

pub struct Column {
    pub i: usize,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.i)
    }
}

impl PhysicalExpr for Column {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        ArrayOrDatum::Array(input.column(self.i).clone())
    }
}

pub struct LiteralLong(pub i64);

impl Display for LiteralLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PhysicalExpr for LiteralLong {
    fn evaluate(&self, _input: &RecordBatch) -> ArrayOrDatum {
        ArrayOrDatum::Datum(Arc::new(Int64Array::new_scalar(self.0)))
    }
}

pub struct LiteralString(pub String);

impl Display for LiteralString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'", self.0)
    }
}

impl PhysicalExpr for LiteralString {
    fn evaluate(&self, _input: &RecordBatch) -> ArrayOrDatum {
        ArrayOrDatum::Datum(Arc::new(StringArray::new_scalar(self.0.clone())))
    }
}

pub struct Eq {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Eq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} == {}", self.l, self.r)
    }
}

impl PhysicalExpr for Eq {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::cmp::eq(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Ne {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Ne {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} != {}", self.l, self.r)
    }
}

impl PhysicalExpr for Ne {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::cmp::neq(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Gt {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Gt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} > {}", self.l, self.r)
    }
}

impl PhysicalExpr for Gt {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::cmp::gt(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Gte {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Gte {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} >= {}", self.l, self.r)
    }
}

impl PhysicalExpr for Gte {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::cmp::gt_eq(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Lt {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Lt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} < {}", self.l, self.r)
    }
}

impl PhysicalExpr for Lt {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::cmp::lt(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Lte {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Lte {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} <= {}", self.l, self.r)
    }
}

impl PhysicalExpr for Lte {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::cmp::lt_eq(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct And {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for And {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} AND {}", self.l, self.r)
    }
}

impl PhysicalExpr for And {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        let b1 = lres
            .get_datum_ref()
            .get()
            .0
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let b2 = rres
            .get_datum_ref()
            .get()
            .0
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        ArrayOrDatum::Array(Arc::new(kernels::boolean::and(b1, b2).unwrap()))
    }
}

pub struct Or {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Or {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} OR {}", self.l, self.r)
    }
}

impl PhysicalExpr for Or {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        let b1 = lres
            .get_datum_ref()
            .get()
            .0
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let b2 = rres
            .get_datum_ref()
            .get()
            .0
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        ArrayOrDatum::Array(Arc::new(kernels::boolean::or(b1, b2).unwrap()))
    }
}

pub struct Add {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Add {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} + {}", self.l, self.r)
    }
}

impl PhysicalExpr for Add {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::numeric::add(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Sub {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Sub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.l, self.r)
    }
}

impl PhysicalExpr for Sub {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::numeric::sub(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Mul {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Mul {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} * {}", self.l, self.r)
    }
}

impl PhysicalExpr for Mul {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::numeric::mul(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Div {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Div {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} / {}", self.l, self.r)
    }
}

impl PhysicalExpr for Div {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::numeric::div(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct Mod {
    pub l: Rc<dyn PhysicalExpr>,
    pub r: Rc<dyn PhysicalExpr>,
}

impl Display for Mod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} % {}", self.l, self.r)
    }
}

impl PhysicalExpr for Mod {
    fn evaluate(&self, input: &RecordBatch) -> ArrayOrDatum {
        let lres = self.l.evaluate(input);
        let rres = self.r.evaluate(input);

        ArrayOrDatum::Array(Arc::new(
            kernels::numeric::rem(lres.get_datum_ref(), rres.get_datum_ref()).unwrap(),
        ))
    }
}

pub struct ScanExec {
    pub ds: Rc<dyn DataSource>,
    pub projection: Vec<String>,
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> SchemaRef {
        let schema = self.ds.schema();
        if self.projection.is_empty() {
            schema
        } else {
            Arc::new(schema_select(schema, &self.projection))
        }
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch>> {
        self.ds.scan(&self.projection)
    }

    fn children(&self) -> Vec<Rc<dyn PhysicalPlan>> {
        vec![]
    }
}

impl Display for ScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScanExec: schema={}, projection=[{}]",
            self.schema(),
            self.projection.join(",")
        )
    }
}

pub struct SelectionExec {
    pub input: Rc<dyn PhysicalPlan>,
    pub expr: Rc<dyn PhysicalExpr>,
}

impl PhysicalPlan for SelectionExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let input = self.input.execute();
        Box::new(input.map(|r| {
            let res = self.expr.evaluate(&r);
            match res {
                ArrayOrDatum::Array(arr) => {
                    let bitvector = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                    let new_cols = r
                        .columns()
                        .iter()
                        .map(|col| kernels::filter::filter(col, bitvector).unwrap())
                        .collect::<Vec<_>>();
                    RecordBatch::try_new(self.schema(), new_cols).unwrap()
                }
                _ => unimplemented!(),
            }
        }))
    }

    fn children(&self) -> Vec<Rc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }
}

impl Display for SelectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SelectionExec: {}", self.expr)
    }
}

pub struct ProjectionExec {
    pub input: Rc<dyn PhysicalPlan>,
    pub expr: Vec<Rc<dyn PhysicalExpr>>,
    pub schema: SchemaRef,
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let input = self.input.execute();
        Box::new(input.map(|r| {
            let new_cols = self
                .expr
                .iter()
                .filter_map(|e| match e.evaluate(&r) {
                    ArrayOrDatum::Array(arr) => Some(arr),
                    ArrayOrDatum::Datum(_) => None,
                })
                .collect::<Vec<_>>();
            RecordBatch::try_new(self.schema(), new_cols).unwrap()
        }))
    }

    fn children(&self) -> Vec<Rc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }
}

impl Display for ProjectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProjectionExec: [{}]",
            self.expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum AggValue {
    Int64(i64),
    String(String),
}

pub trait Accumulator {
    fn accumulate(&mut self, val: AggValue);
    fn final_value(&self) -> AggValue;
}

pub struct MinAccumulator(Option<AggValue>);

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, val: AggValue) {
        match self.0 {
            Some(ref v) => match (v, val) {
                (AggValue::Int64(n), AggValue::Int64(n1)) => {
                    if n1 < *n {
                        self.0 = Some(AggValue::Int64(n1))
                    }
                }
                (AggValue::String(s), AggValue::String(s1)) => {
                    if s1.as_str() < s.as_str() {
                        self.0 = Some(AggValue::String(s1))
                    }
                }
                _ => unimplemented!(),
            },
            None => self.0 = Some(val),
        }
    }

    fn final_value(&self) -> AggValue {
        self.0.clone().unwrap()
    }
}

pub trait AggregateExpr: Display {
    fn input_expr(&self) -> Rc<dyn PhysicalExpr>;
    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>>;
}

pub struct Min(pub Rc<dyn PhysicalExpr>);

impl Display for Min {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MIN({})", self.0)
    }
}

impl AggregateExpr for Min {
    fn input_expr(&self) -> Rc<dyn PhysicalExpr> {
        self.0.clone()
    }

    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>> {
        Rc::new(RefCell::new(MinAccumulator(None)))
    }
}

pub struct HashAggregateExec {
    pub input: Rc<dyn PhysicalPlan>,
    pub group_expr: Vec<Rc<dyn PhysicalExpr>>,
    pub agg_expr: Vec<Rc<dyn AggregateExpr>>,
    pub schema: SchemaRef,
}

impl Display for HashAggregateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Aggregate: groupExpr=[{}], aggregateExpr=[{}]",
            self.group_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.agg_expr
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

impl PhysicalPlan for HashAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = RecordBatch> + '_> {
        let mut map: HashMap<Vec<AggValue>, Vec<Rc<RefCell<dyn Accumulator>>>> = HashMap::new();

        self.input.execute().for_each(|r| {
            let group_keys = self
                .group_expr
                .iter()
                .map(|e| e.evaluate(&r))
                .map(|d| match d {
                    ArrayOrDatum::Array(arr) => arr,
                    _ => unimplemented!(),
                })
                .collect::<Vec<_>>();
            let aggr_input_values = self
                .agg_expr
                .iter()
                .map(|e| e.input_expr().evaluate(&r))
                .map(|d| match d {
                    ArrayOrDatum::Array(arr) => arr,
                    _ => unimplemented!(),
                })
                .collect::<Vec<_>>();

            for i in 0..r.num_rows() {
                let mut row_key = vec![];
                for arr in group_keys.iter() {
                    if let Some(intarr) = arr.as_any().downcast_ref::<Int64Array>() {
                        row_key.push(AggValue::Int64(intarr.value(i)));
                    } else {
                        let strarr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                        row_key.push(AggValue::String(strarr.value(i).to_owned()));
                    }
                }

                let accumulators = self
                    .agg_expr
                    .iter()
                    .map(|e| e.create_accumulator())
                    .collect::<Vec<_>>();
                let entry = map.entry(row_key).or_insert(accumulators);
                for (j, arr) in aggr_input_values.iter().enumerate() {
                    let acc = &*entry[j];
                    if let Some(intarr) = arr.as_any().downcast_ref::<Int64Array>() {
                        acc.borrow_mut()
                            .accumulate(AggValue::Int64(intarr.value(i)));
                    } else {
                        let strarr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                        acc.borrow_mut()
                            .accumulate(AggValue::String(strarr.value(i).to_owned()));
                    }
                }
            }
        });

        let mut builders: Vec<_> = self
            .schema()
            .all_fields()
            .iter()
            .map(|f| -> Box<dyn ArrayBuilder> {
                match f.data_type() {
                    arrow::datatypes::DataType::Utf8 => Box::new(StringBuilder::new()),
                    arrow::datatypes::DataType::Int64 => Box::new(Int64Builder::new()),
                    _ => unimplemented!(),
                }
            })
            .collect();

        for (k, v) in map.iter() {
            let mut index = 0;
            for f in k.iter() {
                match f {
                    AggValue::Int64(n) => {
                        let builder = builders[index]
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap();
                        builder.append_value(*n);
                    }
                    AggValue::String(s) => {
                        let builder = builders[index]
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap();
                        builder.append_value(s);
                    }
                }
                index += 1;
            }

            for acc in v.iter() {
                match acc.borrow().final_value() {
                    AggValue::Int64(n) => {
                        let builder = builders[index]
                            .as_any_mut()
                            .downcast_mut::<Int64Builder>()
                            .unwrap();
                        builder.append_value(n);
                    }
                    AggValue::String(s) => {
                        let builder = builders[index]
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap();
                        builder.append_value(s);
                    }
                }
                index += 1;
            }
        }

        let new_cols: Vec<_> = builders.into_iter().map(|mut b| b.finish()).collect();
        let batch = RecordBatch::try_new(self.schema(), new_cols).unwrap();
        Box::new(vec![batch].into_iter())
    }

    fn children(&self) -> Vec<Rc<dyn PhysicalPlan>> {
        vec![self.input.clone()]
    }
}
