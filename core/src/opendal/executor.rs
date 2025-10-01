use std::any::Any;
use std::sync::{Arc, Mutex};
use std::collections::{BTreeMap, HashMap};
use datafusion::common::Result;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType,
    Statistics, PlanProperties
};
use datafusion::execution::context::TaskContext;
use datafusion::arrow::array::{UInt64Builder, UInt8Builder};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::arrow::record_batch::RecordBatch;
use opendal::services::Fs;
use opendal::Operator;
use opendal::blocking::Operator;
use opendal::Result;
use opendal::ErrorKind;
use r2d2::ManageConnection;
use r2d2::Pool;

struct OpenDALManager {
    builder: opendal::Builder
}
impl ManageConnection for OpenDALManager {
    fn connect(&self) -> Result<blocking::Operator, ErrorKind> {
        let mut builder = self.builder.clone();
        Ok(Operator::new(builder)?.finish())
    }
    fn is_valid(&self, op: blocking::Operator) -> Result<(),ErrorKind> {
        op.check()?
    }
    fn has_broken(&self, op: blocking::Operator) -> bool {
        match op.check() {
            Ok(_) => false,
            Err(_) => true,
        }
    }
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone, Debug)]
pub struct OpenDALDataSource {
    pool: Pool,
}

impl OpenDALDataSource {
    fn new() -> Result<Self,BuildError> {
        let manager = OpenDALManager{
            builder: Fs::default().root("/")
        };
        let pool = Pool::builder()
            .max_size(15)
            .build(manager)?;
        return Self {
            pool: pool
        }
    }
}

#[derive(Debug)]
struct CustomExec {
    db: OpenDALDataSource,
    projected_schema: SchemaRef,
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }


    fn properties(&self) -> &PlanProperties {
        unreachable!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let op = self.pool.get()?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                vec![
                    Arc::new(op.list(".")?.into_iter().map(|entry| entry.name()).collect()),
                ],
            )?],
            self.schema(),
            None,
        )?))
    }
}
