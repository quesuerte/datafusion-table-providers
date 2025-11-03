use std::any::Any;
use std::sync::Arc;
use datafusion::common::Result;
use arrow::error::ArrowError;
use datafusion::physical_plan::{
    project_schema,
    ExecutionPlan, SendableRecordBatchStream, DisplayAs, DisplayFormatType,
    PlanProperties
};
use futures::TryStreamExt;
use futures::stream::StreamExt;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::execution_plan::{EmissionType,Boundedness};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{Array,LargeBinaryArray,StringArray,BooleanArray,UInt64Array,TimestampNanosecondArray};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::datasource::sink::DataSink;
use opendal::Operator;
use opendal::Configurator;
use opendal::EntryMode;
use deadpool::managed;
use deadpool::managed::BuildError;
use std::fmt::Debug;
use datafusion::error::DataFusionError;
use opendal::Error as opendalErr;
use std::error::Error;
use std::fmt::Display;
use std::fmt;
use async_trait::async_trait;
use datafusion::physical_plan::metrics::MetricsSet;
use crate::util::retriable_error::check_and_mark_retriable_error;

#[derive(Debug)]
pub enum OpenDALExecError {
    OpenDALErr(String),
    PoolErr(String),
    ArrowErr(String),
    InvalidSchema(String),
    OverwriteError(String),
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
            OpenDALExecError::InvalidSchema(inner) => write!(f,"{}",inner),
            OpenDALExecError::OverwriteError(inner) => write!(f,"{}",inner),

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
pub struct OpenDALDataSourceInner<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pool: managed::Pool<OpenDALManager<T>>,
    target: String,
    pub schema: SchemaRef,
}

impl<T> OpenDALDataSourceInner<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn new(bldr: T, target: String) -> Result<Self,OpenDALExecError> {
        let manager = OpenDALManager::new(bldr);
        let pool = managed::Pool::builder(manager).build()?;
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("blob", DataType::LargeBinary, true),
            Field::new("is_file", DataType::Boolean, true),
            Field::new("size", DataType::UInt64, true),
            Field::new("content_type", DataType::Utf8, true),
            Field::new("last_modified", DataType::Timestamp(TimeUnit::Nanosecond,None), true),
        ]));
        Ok(Self {
            pool,
            target,
            schema,
           })
    }
}

#[derive(Clone, Debug)]
pub struct OpenDALDataSource<T> 
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub inner: Arc<OpenDALDataSourceInner<T>>,
}

impl<T> OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn new(bldr: T, target: String) -> Result<Self,OpenDALExecError> {
        Ok(Self {
            inner: Arc::new(OpenDALDataSourceInner::new(bldr, target)?),
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
    limit: Option<usize>,

}

impl<T> OpenDALExec<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn try_new(projections: Option<&Vec<usize>>, db: &OpenDALDataSource<T>, limit: Option<usize>) -> Result<Self,DataFusionError> {
        let projected_schema = project_schema(&db.inner.schema, projections)?;
        Ok(Self {
            db: db.clone(),
            projected_schema: projected_schema.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            limit,
        })
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
        let pool = self.db.inner.pool.clone();
        let schema = self.schema();
        let target = self.db.inner.target.clone();
        let limit = self.limit.clone();
        builder.spawn(async move {
            let batch = get_opendal_record_batch(target,schema,pool,limit).await?;
            tx.send(Ok(batch)).await.unwrap();
            Ok(())
        });
        Ok(builder.build())
    }
}

async fn get_opendal_record_batch<T>(target: String, schema: SchemaRef, pool: managed::Pool<OpenDALManager<T>>, limit: Option<usize>) -> Result<RecordBatch> 
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    let op = pool.get().await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;
    RecordBatch::try_new(
        schema.clone(),
        file_lister(target, schema, &op, limit).await.map_err(|e| Into::<DataFusionError>::into(e))?,
    ).map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))
}

async fn file_lister(target: String, schema: SchemaRef, op: &Operator, limit: Option<usize>) -> Result<Vec<Arc<dyn Array>>,OpenDALExecError> {
    let mut rtn = Vec::new();
    let mut lister = {
        let mut builder = op.lister_with(&target);
        if let Some(l) = limit {
            builder = builder.limit(l);
        }
        builder.await?
    };
    let mut path_array: Option<Vec<String>> = schema.fields.find("path").map(|_| Vec::new());
    let mut name_array: Option<Vec<String>> = schema.fields.find("name").map(|_| Vec::new());
    let mut blob_array: Option<Vec<Option<Vec<u8>>>> = schema.fields.find("blob").map(|_| Vec::new());
    let mut file_array: Option<Vec<bool>> = schema.fields.find("is_file").map(|_| Vec::new());
    let mut size_array: Option<Vec<u64>> = schema.fields.find("size").map(|_| Vec::new());
    let mut content_array: Option<Vec<Option<String>>> = schema.fields.find("content_type").map(|_| Vec::new());
    let mut last_modified_array: Option<Vec<Option<i64>>> = schema.fields.find("last_modified").map(|_| Vec::new());

    while let Some(entry) = lister.try_next().await? {
        if entry.metadata().mode() == EntryMode::Unknown {
            continue
        }
        if let Some(ref mut vec) = name_array {
            vec.push(entry.name().to_string());
        }
        let (path, meta) = entry.into_parts();
        if let Some(ref mut vec) = path_array {
            vec.push(path.clone());
        }
        if let Some(ref mut vec) = blob_array {
            if !meta.is_file() {
                vec.push(None);
            } else {
                vec.push(Some(op.read(&path).await?.to_vec()));
            }
        }
        if let Some(ref mut vec) = file_array {
            vec.push(meta.is_file());
        }
        if size_array.is_some() || content_array.is_some() || last_modified_array.is_some() {
            // Looks like these properties aren't loaded unless we call stat
            let meta2 = op.stat(&path).await?;
            if let Some(ref mut vec) = size_array {
                vec.push(meta2.content_length());
            }
            if let Some(ref mut vec) = content_array {
                vec.push(meta2.content_type().map(str::to_string));
            }
            if let Some(ref mut vec) = last_modified_array {
                vec.push(meta2.last_modified().and_then(|t| t.timestamp_nanos_opt()));
            }
        }
    }
    if let Some(vec) = path_array {
        rtn.push(Arc::new(StringArray::from(vec)) as Arc<dyn Array>);
    }
    if let Some(vec) = name_array {
        rtn.push(Arc::new(StringArray::from(vec)) as Arc<dyn Array>);
    }
    if let Some(vec) = blob_array {
        let v_refs: Vec<Option<&[u8]>> = vec.iter().map(|b| b.as_deref()).collect();
        rtn.push(Arc::new(LargeBinaryArray::from_opt_vec(v_refs)) as Arc<dyn Array>);
    }
    if let Some(vec) = file_array {
        rtn.push(Arc::new(BooleanArray::from(vec)) as Arc<dyn Array>);
    }
    if let Some(vec) = size_array {
        rtn.push(Arc::new(UInt64Array::from(vec)) as Arc<dyn Array>);
    }
    if let Some(vec) = content_array {
        rtn.push(Arc::new(StringArray::from(vec)) as Arc<dyn Array>);
    }
    if let Some(vec) = last_modified_array {
        rtn.push(Arc::new(TimestampNanosecondArray::from(vec)) as Arc<dyn Array>);
    }
    Ok(rtn)
}

#[derive(Clone,Debug)]
pub struct OpenDALDataSink<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    db: OpenDALDataSource<T>,
    overwrite: InsertOp,
}

#[async_trait]
impl<T> DataSink for OpenDALDataSink<T> 
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.db.inner.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut num_files = 0;
        let op = self.db.inner.pool.get().await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;

        // Should we replace files if they already exist, or throw error
        let replace = matches!(self.overwrite, InsertOp::Overwrite | InsertOp::Replace);

        // Check that schema has what we need in it
        let schema: SchemaRef = data.schema();
        let path_column_index = schema.index_of("path").map_err(|_| Into::<DataFusionError>::into(OpenDALExecError::InvalidSchema("Insert operations must reference `path` column".to_string())))?;
        let blob_column_index = schema.index_of("blob").map_err(|_| Into::<DataFusionError>::into(OpenDALExecError::InvalidSchema("Insert operations must reference `blob` column and provide binary data".to_string())))?;

        if !matches!(schema.field(path_column_index).data_type(), DataType::Utf8) || !matches!(schema.field(blob_column_index).data_type(), DataType::LargeBinary) {
            return Err(Into::<DataFusionError>::into(OpenDALExecError::InvalidSchema("Path must be of type string, and blob must be binary".to_string())));
        }

        while let Some(batch_result) = data.next().await {
            let batch = batch_result.map_err(check_and_mark_retriable_error)?;

            let path_col = batch
                .column(path_column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Into::<DataFusionError>::into(OpenDALExecError::InvalidSchema("`path` must be StringArray".to_string())))?;
            if let Some(bin_col) = batch.column(blob_column_index).as_any().downcast_ref::<LargeBinaryArray>() {
                for i in 0..batch.num_rows() {
                    let p = path_col.value(i);
                    let blob = bin_col.value(i).to_vec();
                    write_one(&op,&p, blob, replace).await.map_err(Into::<DataFusionError>::into)?;
                    num_files += 1;
                }
            }
        }
        Ok(num_files)
    }
}

async fn write_one(op: &Operator, path: &str, data: impl Into<Vec<u8>>, replace: bool) -> Result<(),OpenDALExecError> {
    if op.exists(path).await? && !replace {
        return Err(OpenDALExecError::OverwriteError(format!("Path {} exists and overwrite not allowed",path)));
    }
    op.write(path, data.into()).await?;
    Ok(())
}

impl<T> OpenDALDataSink<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn new(
        db: &OpenDALDataSource<T>,
        overwrite: InsertOp,
    ) -> Self {
        Self {
            db: db.clone(),
            overwrite,
        }
    }
}

impl<T> DisplayAs for OpenDALDataSink<T> 
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "OpenDALDataSink")
    }
}
