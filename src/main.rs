use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::error::Result;
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;
use rand::Rng;


pub fn set_up_maths_data_test() -> Result<SessionContext> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("index", DataType::UInt8, false),
        Field::new("uint", DataType::UInt64, true),
        Field::new("int", DataType::Int64, true),
        Field::new("float", DataType::Float64, true),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt8Array::from_iter_values([1, 2, 3])),
            Arc::new(UInt64Array::from(vec![Some(2), Some(3), None])),
            Arc::new(Int64Array::from(vec![Some(-2), Some(3), None])),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(3.3), None])),
        ],
    )?;

    // declare a new context
    let ctx = SessionContext::new();
    ctx.register_batch("data_table", batch)?;
    // declare a table in memory.
    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {

    pub fn random_normal(_args: &[ArrayRef]) -> Result<ArrayRef> {

        let mut float64array_builder = Float64Array::builder(1);
        let mut rng = rand::thread_rng();
        let n: u32 = rng.gen_range(0..100);
        float64array_builder.append_value(n as f64);
        Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
    }

    let ctx = set_up_maths_data_test()?;
    let udf = make_scalar_function(random_normal);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
    let random_normal_udf = ScalarUDF::new(
        "random_normal",
        &Signature::any(0, Volatility::Volatile),
        &return_type,
        &udf,
    );

    ctx.register_udf(random_normal_udf);

    let df = ctx
        .sql(
            r#"SELECT * FROM data_table"#,
        )
        .await?;

    df.clone().show().await?;

    let df = ctx
        .sql(
            r#"SELECT random_normal() AS random_normal, random() AS native_random FROM data_table"#,
        )
        .await?;

    df.clone().show().await?;

    Ok(())
}
