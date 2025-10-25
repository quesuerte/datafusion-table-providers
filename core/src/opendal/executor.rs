use std::any::Any;
use std::sync::Arc;
use arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType,
    PlanProperties
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::execution_plan::{EmissionType,Boundedness};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::StringArray;
use opendal::Operator;
use opendal::Configurator;
use deadpool::managed;
use deadpool::managed::BuildError;
use std::fmt::Debug;
use datafusion::error::DataFusionError;
use opendal::Error as opendalErr;
use std::error::Error;
use std::fmt::Display;
use std::fmt;

#[derive(Debug)]
pub enum OpenDALExecError {
    OpenDALErr(String),
    PoolErr(String),
    ArrowErr(String),
}

impl From<opendalErr> for OpenDALExecError {
    fn from(err: opendalErr) -> Self {
        Self::OpenDALErr(format!("{:?}",err))
    }
}

impl<T: Debug> From<managed::PoolError<T>> for OpenDALExecError {
    fn from(err: managed::PoolError<T>) -> Self {
        Self::PoolErr(format!("{:?}",err))
    }
}

impl From<ArrowError> for OpenDALExecError {
    fn from(err: ArrowError) -> Self {
        Self::ArrowErr(format!("{:?}",err))
    }
}

impl From<BuildError> for OpenDALExecError {
    fn from(err: BuildError) -> Self {
        Self::PoolErr(format!("{:?}",err))
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
            OpenDALExecError::PoolErr(inner) => write!(f, "{}", inner),
            OpenDALExecError::ArrowErr(inner) => write!(f, "{}", inner),
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

impl<T> managed::Manager for OpenDALManager<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    type Type = Operator;
    type Error = OpenDALExecError;

    async fn create(&self) -> Result<Operator, OpenDALExecError> {
        let cfg = self.builder.clone();
        Ok(Operator::from_config(cfg)?.finish())
    }

    async fn recycle(&self, _: &mut Operator, _: &managed::Metrics) -> managed::RecycleResult<OpenDALExecError> {
        Ok(())
    }
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone, Debug)]
pub struct OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pool: managed::Pool<OpenDALManager<T>>,
}

impl<T> OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn new(bldr: T) -> Result<Self,OpenDALExecError> {
        let manager = OpenDALManager::new(bldr);
        let pool = managed::Pool::builder(manager).build()?;
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
    pub properties: PlanProperties,
}

impl<T> OpenDALExec<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: OpenDALDataSource<T>) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        Self {
            db,
            projected_schema: projected_schema.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
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
        &self.properties
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
        let mut builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&self.projected_schema), 10);
        let tx = builder.tx();
        let pool = self.db.pool.clone();
        let schema = self.schema();
        builder.spawn(async move {
            let batch = get_opendal_record_batch(schema,pool).await?;
            tx.send(Ok(batch)).await.unwrap();
            Ok(())
        });
        Ok(builder.build())
    }
}

async fn get_opendal_record_batch<T>(schema: SchemaRef, pool: managed::Pool<OpenDALManager<T>>) -> Result<RecordBatch> 
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    let op = pool.get().await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;
    let array = Arc::new(StringArray::from_iter_values(
        op.list(".").await
            .map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?
            .into_iter()
            .map(|entry| entry.name().to_owned()),
    ));
    RecordBatch::try_new(
        schema,
        vec![array],
    ).map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))
}
