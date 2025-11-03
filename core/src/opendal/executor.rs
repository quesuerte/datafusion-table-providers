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

// Field names
static PATH_COL: &str = "path";
static NAME_COL: &str = "name";
static BLOB_COL: &str = "blob";
static IS_FILE_COL: &str = "is_file";
static SIZE_COL: &str = "size";
static CONTENT_TYPE_COL: &str = "content_type";
static LAST_MODIFIED_COL: &str = "last_modified";

// At this point, these determine global batch sizes for read operations
// New batches will start either every 1000 rows, or every 10 MB of blob file if they're projected,
// whichever comes first
static BATCH_SIZE_THRESHOLD: u64 = 100 * 1024 * 1024; // e.g., 10 MB
static ROW_COUNT_THRESHOLD: usize = 1000;


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
            Field::new(PATH_COL, DataType::Utf8, false),
            Field::new(NAME_COL, DataType::Utf8, true),
            Field::new(BLOB_COL, DataType::LargeBinary, true),
            Field::new(IS_FILE_COL, DataType::Boolean, true),
            Field::new(SIZE_COL, DataType::UInt64, true),
            Field::new(CONTENT_TYPE_COL, DataType::Utf8, true),
            Field::new(LAST_MODIFIED_COL, DataType::Timestamp(TimeUnit::Nanosecond,None), true),
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
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            Arc::clone(&self.projected_schema),
            10, // channel buffer
        );
        let tx = builder.tx();
        let pool = self.db.inner.pool.clone();
        let target = self.db.inner.target.clone();
        let limit = self.limit.clone();
        let p_sch = Arc::clone(&self.projected_schema);

        builder.spawn(async move {
            let op = pool
                .get()
                .await
                .map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;

            let mut lister = {
                let mut builder = op.lister_with(&target);
                if let Some(l) = limit {
                    builder = builder.limit(l);
                }
                builder.await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?
            };

            // These correspond to columns
            let mut path_array = Vec::new();
            let mut name_array = Vec::new();
            let mut blob_array = Vec::new();
            let mut file_array = Vec::new();
            let mut size_array = Vec::new();
            let mut content_array = Vec::new();
            let mut last_modified_array = Vec::new();

            let mut accumulated_size = 0u64;
            let mut row_count = 0usize;

            while let Some(entry) = lister.try_next().await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))? {
                if entry.metadata().mode() == EntryMode::Unknown {
                    continue;
                }

                if p_sch.fields.find(NAME_COL).is_some() {
                    name_array.push(entry.name().to_string());
                }

                let (path, meta) = entry.into_parts();
                let is_file = meta.is_file();

                if p_sch.fields.find(PATH_COL).is_some() {
                    path_array.push(path.clone());
                }
                if p_sch.fields.find(IS_FILE_COL).is_some() {
                    file_array.push(is_file);
                }

                // collect metadata
                let meta2 = op.stat(&path).await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;
                let sz = meta2.content_length();
                accumulated_size += sz;
                if p_sch.fields.find(SIZE_COL).is_some() {
                    size_array.push(sz);
                }
                if p_sch.fields.find(CONTENT_TYPE_COL).is_some() {
                    content_array.push(meta2.content_type().map(str::to_string));
                }
                if p_sch.fields.find(LAST_MODIFIED_COL).is_some() {
                    last_modified_array.push(meta2.last_modified().and_then(|t| t.timestamp_nanos_opt()));
                }

                let include_blob = p_sch.fields.find(BLOB_COL).is_some();
                if include_blob && is_file {
                    blob_array.push(Some(op.read(&path).await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?.to_vec()));
                } else {
                    blob_array.push(None);
                }

                row_count += 1;

                let should_flush =  if include_blob {
                    accumulated_size >= BATCH_SIZE_THRESHOLD || row_count >= ROW_COUNT_THRESHOLD
                } else {
                    row_count >= ROW_COUNT_THRESHOLD
                };

                // flush if we exceed threshold
                if should_flush {
                    let batch = build_batch(
                        &p_sch,
                        &path_array,
                        &name_array,
                        &blob_array,
                        &file_array,
                        &size_array,
                        &content_array,
                        &last_modified_array,
                    )?;

                    tx.send(Ok(batch)).await.unwrap();

                    // reset buffers
                    path_array.clear();
                    name_array.clear();
                    blob_array.clear();
                    file_array.clear();
                    size_array.clear();
                    content_array.clear();
                    last_modified_array.clear();
                    accumulated_size = 0;
                    row_count = 0;
                }
            }

            // send final partial batch if any
            if !path_array.is_empty() {
                let batch = build_batch(
                    &p_sch,
                    &path_array,
                    &name_array,
                    &blob_array,
                    &file_array,
                    &size_array,
                    &content_array,
                    &last_modified_array,
                )?;
                tx.send(Ok(batch)).await.unwrap();
            }

            Ok(())
        });

        Ok(builder.build())
    }
}

fn build_batch(
    schema: &SchemaRef,
    paths: &[String],
    names: &[String],
    blobs: &[Option<Vec<u8>>],
    is_files: &[bool],
    sizes: &[u64],
    content_types: &[Option<String>],
    last_modified: &[Option<i64>],
) -> Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    if schema.fields.find(PATH_COL).is_some() {
        columns.push(Arc::new(StringArray::from(paths.to_vec())));
    }
    if schema.fields.find(NAME_COL).is_some() {
        columns.push(Arc::new(StringArray::from(names.to_vec())));
    }
    if schema.fields.find(BLOB_COL).is_some() {
        let refs: Vec<Option<&[u8]>> = blobs.iter().map(|b| b.as_deref()).collect();
        columns.push(Arc::new(LargeBinaryArray::from_opt_vec(refs)));
    }
    if schema.fields.find(IS_FILE_COL).is_some() {
        columns.push(Arc::new(BooleanArray::from(is_files.to_vec())));
    }
    if schema.fields.find(SIZE_COL).is_some() {
        columns.push(Arc::new(UInt64Array::from(sizes.to_vec())));
    }
    if schema.fields.find(CONTENT_TYPE_COL).is_some() {
        columns.push(Arc::new(StringArray::from(content_types.to_vec())));
    }
    if schema.fields.find(LAST_MODIFIED_COL).is_some() {
        columns.push(Arc::new(TimestampNanosecondArray::from(last_modified.to_vec())));
    }
    if columns.is_empty() {
        // DataFusion expects at least one column per batch.
        let dummy: Vec<bool> = std::iter::repeat(true).take(paths.len().max(1)).collect();
        columns.push(Arc::new(BooleanArray::from(dummy)) as Arc<dyn Array>);
    }

    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
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
        let path_column_index = schema.index_of(PATH_COL).map_err(|_| Into::<DataFusionError>::into(OpenDALExecError::InvalidSchema("Insert operations must reference `path` column".to_string())))?;
        let blob_column_index = schema.index_of(BLOB_COL).map_err(|_| Into::<DataFusionError>::into(OpenDALExecError::InvalidSchema("Insert operations must reference `blob` column and provide binary data".to_string())))?;

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
