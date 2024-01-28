use std::iter;
use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array, UInt8Array};
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

#[derive(Debug)]
pub struct RandomNormal {
    signature: Signature
}

impl RandomNormal {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![Any(0), Variadic(vec![Float64])], Volatility::Volatile)
        }
    }
}

impl ScalarUDFImpl for RandomNormal {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn name(&self) -> &str {
        "random_normal"
    }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        dbg!(&args[0]);

        let len: usize = match &args[0] {
            ColumnarValue::Array(array) => array.len(),
            _ => return panic!("Opsies"),
        };
        let mut rng = thread_rng();
        let values = iter::repeat_with(|| rng.gen_range(0.0..1.0)).take(len);
        let array = Float64Array::from_iter_values(values);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}


#[tokio::main]
async fn main() -> Result<()> {

    // pub fn random_normal(_args: &[ArrayRef]) -> Result<ArrayRef> {
    //
    //     let mut float64array_builder = Float64Array::builder(1);
    //     let mut rng = rand::thread_rng();
    //     let n: u32 = rng.gen_range(0..100);
    //     float64array_builder.append_value(n as f64);
    //     Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
    // }

    let ctx = set_up_maths_data_test()?;

    let random_normal_udf = ScalarUDF::from(RandomNormal::new());

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
