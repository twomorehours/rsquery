use std::rc::Rc;

use crate::{
    logical_plan::{self, LogicalExpr, LogicalPlan},
    physical_plan::{
        self, AggregateExpr, PhysicalExpr, PhysicalPlan, ProjectionExec, ScanExec, SelectionExec,
    },
};

fn create_physical_expr(
    expr: Rc<dyn LogicalExpr>,
    input: Rc<dyn LogicalPlan>,
) -> Rc<dyn PhysicalExpr> {
    if let Some(e) = expr.as_any().downcast_ref::<logical_plan::LiteralLong>() {
        return Rc::new(physical_plan::LiteralLong(e.0));
    }
    if let Some(e) = expr.as_any().downcast_ref::<logical_plan::LiteralString>() {
        return Rc::new(physical_plan::LiteralString(e.0.clone()));
    }
    if let Some(e) = expr.as_any().downcast_ref::<logical_plan::Column>() {
        let i = input
            .schema()
            .all_fields()
            .iter()
            .enumerate()
            .find_map(|(i, c)| if c.name() == &e.name { Some(i) } else { None })
            .unwrap();
        return Rc::new(physical_plan::Column { i });
    }

    if let Some(logical_plan::Eq {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Eq>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Eq { l, r });
    }
    if let Some(logical_plan::Ne {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Ne>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Ne { l, r });
    }
    if let Some(logical_plan::Gt {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Gt>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Gt { l, r });
    }
    if let Some(logical_plan::Gte {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Gte>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Gte { l, r });
    }
    if let Some(logical_plan::Lte {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Lte>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Lte { l, r });
    }
    if let Some(logical_plan::Lt {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Lt>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Lt { l, r });
    }
    if let Some(logical_plan::And {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::And>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::And { l, r });
    }
    if let Some(logical_plan::Or {
        base: logical_plan::BooleanBinaryExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Or>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Or { l, r });
    }
    if let Some(logical_plan::Add {
        base: logical_plan::MathExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Add>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Add { l, r });
    }
    if let Some(logical_plan::Sub {
        base: logical_plan::MathExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Sub>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Sub { l, r });
    }
    if let Some(logical_plan::Mul {
        base: logical_plan::MathExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Mul>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Mul { l, r });
    }
    if let Some(logical_plan::Div {
        base: logical_plan::MathExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Div>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Div { l, r });
    }
    if let Some(logical_plan::Mod {
        base: logical_plan::MathExpr { base },
    }) = expr.as_any().downcast_ref::<logical_plan::Mod>()
    {
        let l = create_physical_expr(base.l.clone(), input.clone());
        let r = create_physical_expr(base.r.clone(), input.clone());

        return Rc::new(physical_plan::Mod { l, r });
    }

    unimplemented!()
}

pub fn create_physical_plan(plan: Rc<dyn LogicalPlan>) -> Rc<dyn PhysicalPlan> {
    if let Some(p) = plan.as_any().downcast_ref::<logical_plan::Scan>() {
        return Rc::new(ScanExec {
            ds: p.data_source.clone(),
            projection: p.projection.clone(),
        });
    }
    if let Some(p) = plan.as_any().downcast_ref::<logical_plan::Selection>() {
        let input = create_physical_plan(p.input.clone());
        let expr = create_physical_expr(p.expr.clone(), p.input.clone());
        return Rc::new(SelectionExec { input, expr });
    }
    if let Some(p) = plan.as_any().downcast_ref::<logical_plan::Projection>() {
        let input = create_physical_plan(p.input.clone());
        let expr = p
            .expr
            .iter()
            .map(|e| create_physical_expr(e.clone(), p.input.clone()))
            .collect::<Vec<_>>();
        return Rc::new(ProjectionExec {
            input,
            expr,
            schema: p.schema(),
        });
    }
    if let Some(p) = plan.as_any().downcast_ref::<logical_plan::Aggregate>() {
        let input = create_physical_plan(p.input.clone());
        let group_expr = p
            .group_expr
            .iter()
            .map(|e| create_physical_expr(e.clone(), p.input.clone()))
            .collect::<Vec<_>>();
        let agg_expr = p
            .agg_expr
            .iter()
            .map(|e| -> Rc<dyn AggregateExpr> {
                if let Some(min) = e.as_any().downcast_ref::<logical_plan::Min>() {
                    let pe = create_physical_expr(min.expr.clone(), p.input.clone());
                    return Rc::new(physical_plan::Min(pe));
                }
                unimplemented!()
            })
            .collect::<Vec<_>>();
        return Rc::new(physical_plan::HashAggregateExec {
            input,
            group_expr,
            agg_expr,
            schema: p.schema(),
        });
    }
    unimplemented!()
}
