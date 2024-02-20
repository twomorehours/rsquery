use std::{any::Any, fmt::Display, rc::Rc, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::{data_source::DataSource, schema_select};

pub trait LogicalPlan: Display {
    fn schema(&self) -> SchemaRef;
    fn children(&self) -> Vec<Rc<dyn LogicalPlan>>;
    fn as_any(&self) -> &dyn Any;
}

pub trait LogicalExpr: Display {
    fn to_field(&self, input: Rc<dyn LogicalPlan>) -> Field;
    fn as_any(&self) -> &dyn Any;
}

pub struct Column {
    pub name: String,
}

pub fn col(name: String) -> Column {
    Column { name }
}

impl LogicalExpr for Column {
    fn to_field(&self, input: Rc<dyn LogicalPlan>) -> Field {
        input
            .schema()
            .all_fields()
            .iter()
            .find(|f| f.name() == &self.name)
            .map(|&f| f.clone())
            .unwrap()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.name)
    }
}

pub struct LiteralString(pub String);

pub fn string_lit(str: String) -> LiteralString {
    LiteralString(str)
}

impl LogicalExpr for LiteralString {
    fn to_field(&self, _: Rc<dyn LogicalPlan>) -> Field {
        Field::new(self.0.clone(), DataType::Utf8, false)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for LiteralString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'", self.0)
    }
}

pub struct LiteralLong(pub i64);

impl LiteralLong {
    pub fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn long_lit(n: i64) -> LiteralLong {
    LiteralLong(n)
}

impl LogicalExpr for LiteralLong {
    fn to_field(&self, _: Rc<dyn LogicalPlan>) -> Field {
        Field::new(self.0.to_string(), DataType::Int64, false)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for LiteralLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct BooleanBinaryExpr {
    pub base: BinaryExpr,
}

pub struct BinaryExpr {
    pub name: String,
    pub op: String,
    pub l: Rc<dyn LogicalExpr>,
    pub r: Rc<dyn LogicalExpr>,
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.l, self.op, self.r)
    }
}

impl Display for BooleanBinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.base.fmt(f)
    }
}

impl LogicalExpr for BooleanBinaryExpr {
    fn to_field(&self, _input: Rc<dyn LogicalPlan>) -> Field {
        Field::new(self.base.name.clone(), DataType::Boolean, false)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

macro_rules! create_boolean_binary_expr {
    ($struct_name:ident, $fname:literal, $op: literal) => {
        pub struct $struct_name {
            pub base: BooleanBinaryExpr,
        }

        impl $struct_name {
            pub fn new(l: Rc<dyn LogicalExpr>, r: Rc<dyn LogicalExpr>) -> Self {
                Self {
                    base: BooleanBinaryExpr {
                        base: BinaryExpr {
                            name: $fname.to_string(),
                            op: $op.to_string(),
                            l,
                            r,
                        },
                    },
                }
            }
        }

        impl Display for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.base.fmt(f)
            }
        }

        impl LogicalExpr for $struct_name {
            fn to_field(&self, input: Rc<dyn LogicalPlan>) -> Field {
                self.base.to_field(input)
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    };
}

create_boolean_binary_expr!(And, "and", "AND");
create_boolean_binary_expr!(Or, "or", "OR");
create_boolean_binary_expr!(Eq, "eq", "==");
create_boolean_binary_expr!(Ne, "ne", "!=");
create_boolean_binary_expr!(Gt, "gt", ">");
create_boolean_binary_expr!(Gte, "gte", ">=");
create_boolean_binary_expr!(Lt, "lt", "<");
create_boolean_binary_expr!(Lte, "lte", "<=");

pub struct MathExpr {
    pub base: BinaryExpr,
}

impl Display for MathExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.base.fmt(f)
    }
}

impl LogicalExpr for MathExpr {
    fn to_field(&self, input: Rc<dyn LogicalPlan>) -> Field {
        self.base.l.to_field(input)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

macro_rules! create_math_binary_expr {
    ($struct_name:ident, $fname:literal, $op: literal) => {
        pub struct $struct_name {
            pub base: MathExpr,
        }

        impl $struct_name {
            pub fn new(l: Rc<dyn LogicalExpr>, r: Rc<dyn LogicalExpr>) -> Self {
                Self {
                    base: MathExpr {
                        base: BinaryExpr {
                            name: $fname.to_string(),
                            op: $op.to_string(),
                            l,
                            r,
                        },
                    },
                }
            }
        }

        impl Display for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.base.fmt(f)
            }
        }

        impl LogicalExpr for $struct_name {
            fn to_field(&self, input: Rc<dyn LogicalPlan>) -> Field {
                self.base.to_field(input)
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
        }
    };
}

create_math_binary_expr!(Add, "add", "+");
create_math_binary_expr!(Sub, "sub", "-");
create_math_binary_expr!(Mul, "mul", "*");
create_math_binary_expr!(Div, "div", "/");
create_math_binary_expr!(Mod, "mod", "%");

pub fn pretty_print_plan(plan: Rc<dyn LogicalPlan>) -> String {
    _pretty_print_plan(plan, 0)
}
fn _pretty_print_plan(plan: Rc<dyn LogicalPlan>, indent: u32) -> String {
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

pub struct Scan {
    pub path: String,
    pub data_source: Rc<dyn DataSource>,
    pub projection: Vec<String>,
}

impl Scan {
    pub fn new(path: String, data_source: Rc<dyn DataSource>, projection: Vec<String>) -> Scan {
        Self {
            path,
            data_source,
            projection,
        }
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> SchemaRef {
        if self.projection.is_empty() {
            self.data_source.schema()
        } else {
            Arc::new(schema_select(self.data_source.schema(), &self.projection))
        }
    }

    fn children(&self) -> Vec<Rc<dyn LogicalPlan>> {
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.projection.is_empty() {
            write!(f, "Scan: {} projection=None", self.path)
        } else {
            write!(
                f,
                "Scan: {} projection=[{}]",
                self.path,
                self.projection.join(",")
            )
        }
    }
}

pub struct Projection {
    pub input: Rc<dyn LogicalPlan>,
    pub expr: Vec<Rc<dyn LogicalExpr>>,
}

impl Projection {
    pub fn new(input: Rc<dyn LogicalPlan>, expr: Vec<Rc<dyn LogicalExpr>>) -> Self {
        Self { input, expr }
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(
            self.expr
                .iter()
                .map(|e| e.to_field(self.input.clone()))
                .collect::<Vec<_>>(),
        ))
    }

    fn children(&self) -> Vec<Rc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Projection: {}",
            self.expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

pub struct Selection {
    pub input: Rc<dyn LogicalPlan>,
    pub expr: Rc<dyn LogicalExpr>,
}

impl Selection {
    pub fn new(input: Rc<dyn LogicalPlan>, expr: Rc<dyn LogicalExpr>) -> Self {
        Self { input, expr }
    }
}

impl LogicalPlan for Selection {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Rc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Selection: {}", self.expr)
    }
}

pub struct Min {
    pub name: String,
    pub expr: Rc<dyn LogicalExpr>,
}

impl Display for Min {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MIN({}) as {}", self.expr, self.name)
    }
}

impl LogicalExpr for Min {
    fn to_field(&self, input: Rc<dyn LogicalPlan>) -> Field {
        let f = self.expr.to_field(input);
        f.with_name(self.name.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct Aggregate {
    pub input: Rc<dyn LogicalPlan>,
    pub group_expr: Vec<Rc<dyn LogicalExpr>>,
    pub agg_expr: Vec<Rc<dyn LogicalExpr>>,
}

impl Display for Aggregate {
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

impl LogicalPlan for Aggregate {
    fn schema(&self) -> SchemaRef {
        let mut fields = vec![];
        self.group_expr
            .iter()
            .map(|e| e.to_field(self.input.clone()))
            .for_each(|f| fields.push(f));
        self.agg_expr
            .iter()
            .map(|e| e.to_field(self.input.clone()))
            .for_each(|f| fields.push(f));
        Arc::new(Schema::new(fields))
    }

    fn children(&self) -> Vec<Rc<dyn LogicalPlan>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct DataFrame(pub Rc<dyn LogicalPlan>);

impl DataFrame {
    pub fn project(self, expr: Vec<Rc<dyn LogicalExpr>>) -> Self {
        Self(Rc::new(Projection::new(self.0, expr)))
    }

    pub fn filter(self, expr: Rc<dyn LogicalExpr>) -> Self {
        Self(Rc::new(Selection::new(self.0, expr)))
    }

    pub fn aggregate(
        self,
        group_expr: Vec<Rc<dyn LogicalExpr>>,
        agg_expr: Vec<Rc<dyn LogicalExpr>>,
    ) -> Self {
        Self(Rc::new(Aggregate {
            input: self.0,
            agg_expr,
            group_expr,
        }))
    }

    pub fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    pub fn logic_plan(&self) -> Rc<dyn LogicalPlan> {
        self.0.clone()
    }
}
