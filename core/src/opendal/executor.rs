use std::any::Any;
use std::sync::Arc;
use datafusion::common::Result;
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType,
    PlanProperties
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::StringArray;
use opendal::Operator;
use opendal::blocking::Operator as Blop;
use opendal::Configurator;
use r2d2::ManageConnection;
use r2d2::Pool;
use std::fmt::Debug;
use datafusion::error::DataFusionError;
use opendal::Error as opendalErr;
use r2d2::Error as r2d2Err;
use std::error::Error;
use std::fmt::Display;
use std::fmt;

#[derive(Debug)]
pub enum OpenDALExecError {
    OpenDALErr(String),
    R2D2Err(String),
}

impl From<r2d2Err> for OpenDALExecError {
    fn from(err: r2d2Err) -> Self {
        Self::R2D2Err(format!("{:?}",err))
    }
}

impl From<opendalErr> for OpenDALExecError {
    fn from(err: opendalErr) -> Self {
        Self::OpenDALErr(format!("{:?}",err))
    }
}

impl Into<DataFusionError> for OpenDALExecError {
    fn into(self) -> DataFusionError {
        DataFusionError::Execution(format!("{:?}",self))
    }
}

impl Error for OpenDALExecError {}

impl Display for OpenDALExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenDALExecError::OpenDALErr(inner) => write!(f, "{}", inner),
            OpenDALExecError::R2D2Err(inner) => write!(f,"{}", inner)
        }
    }
}

#[derive(Clone, Debug)]
struct OpenDALManager<T: Configurator + Clone + Send + Sync + 'static + Debug> {
    builder: T,
}

impl<T: Configurator + Clone + Send + Sync + 'static + Debug> OpenDALManager<T> {
    fn new(bldr: T) -> Self {
        Self { builder: bldr }
    }
}

/// Implement r2d2::ManageConnection properly
impl<T> ManageConnection for OpenDALManager<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    type Connection = Blop;
    type Error = OpenDALExecError;
    fn connect(&self) -> Result<Self::Connection, OpenDALExecError> {
        let builder = self.builder.clone();
        Ok(Blop::new(Operator::from_config(builder)?.finish())?)
    }
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(),OpenDALExecError> {
        Ok(conn.check()?)
    }
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.check().is_err()
    }
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone, Debug)]
pub struct OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pool: Pool<OpenDALManager<T>>,
}

impl<T> OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn new(bldr: T) -> Result<Self,OpenDALExecError> {
        let manager = OpenDALManager::new(bldr);
        let pool = Pool::builder()
            .max_size(15)
            .build(manager)?;
        Ok(Self {
            pool: pool
           })
    }
}

#[derive(Debug)]
pub struct OpenDALExec<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub db: OpenDALDataSource<T>,
    pub projected_schema: SchemaRef,
}

impl<T> DisplayAs for OpenDALExec<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OpenDALExec")
    }
}

impl<T> ExecutionPlan for OpenDALExec<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn name(&self) -> &str {
        "OpenDALExec"
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
        let op = self.db.pool.get().map_err(|e: r2d2::Error| <OpenDALExecError as Into<DataFusionError>>::into(OpenDALExecError::from(e)))?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                vec![
                    Arc::new(StringArray::from(op.list(".").map_err(|e: opendal::Error| <OpenDALExecError as Into<DataFusionError>>::into(OpenDALExecError::from(e)))?.into_iter().map(|entry| entry.name().to_owned()).collect::<Vec<String>>())),
                ],
            )?],
            self.schema(),
            None,
        )?))
    }
}
