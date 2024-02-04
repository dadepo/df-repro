use std::iter;
use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::datatypes::DataType::Float64;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, Volatility};
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::error::Result;
use datafusion::logical_expr::TypeSignature::{Any, Variadic};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::physical_plan::ColumnarValue;
use datafusion::prelude::SessionContext;
use rand::{Rng, thread_rng};


pub fn set_up_data_test() -> Result<SessionContext> {

    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("index", DataType::UInt8, false),
        Field::new("json_data", DataType::Utf8, true),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt8Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec![
                Some(r#" { "this" : "is", "a": [ "test" ] } "#),
                Some(r#"{"a":[2,3.5,true,false,null,"x"]}"#),
                Some(r#"[ "one", "two" ]"#),
                Some(r#"123"#),
                Some(r#"12.3"#),
                Some(r#"true"#),
                Some(r#"false"#),
                None,
            ])),
        ],
    )?;

    // declare a new context
    let ctx = SessionContext::new();
    // declare a table in memory.
    ctx.register_batch("json_table", batch)?;

    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("index", DataType::UInt8, false),
        Field::new("json_data", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt8Array::from_iter_values([1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec![
                Some(r#" { "this" : "is", "a": [ "test" ] } "#),
                Some(r#"{"a":[2,3.5,true,false,null,"x"]}"#),
                Some(r#"[ "one", "two" ]"#),
                Some(r#"123"#),
                Some(r#"12.3"#),
                Some(r#"true"#),
                Some(r#"false"#),
                None,
            ])),
        ],
    )?;

    // declare a new context
    let ctx = SessionContext::new();
    // declare a table in memory.
    ctx.register_batch("json_value_table", batch)?;

    Ok(ctx)
}

#[tokio::main]
async fn main() -> Result<()> {

    let ctx = set_up_data_test()?;

    // Fails with Error: SQL(ParserError("Expected (, found: EOF"), None)
    let df = ctx
        .sql(
            r#"SELECT * FROM json_table"#,
        )
        .await?;

    df.clone().show().await?;


    Ok(())
}
