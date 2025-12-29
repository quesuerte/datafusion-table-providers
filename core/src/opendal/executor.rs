use std::{any::Any,path::Path,sync::Arc,fmt::Debug,error::Error,fmt,fmt::Display,collections::HashMap};
use datafusion::common::{Result,ScalarValue};
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
use datafusion::logical_expr::{dml::InsertOp,Expr,Operator as ExprOp, BinaryExpr};
use datafusion::datasource::sink::DataSink;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::MetricsSet;
use opendal::{Operator,Configurator,EntryMode, Error as opendalErr};
use deadpool::{managed,managed::BuildError};
use async_trait::async_trait;
use crate::util::retriable_error::check_and_mark_retriable_error;
use tokio::sync::mpsc::error::SendError;

// Field names
const URI_COL: &str = "uri";
const NAMESPACE_COL: &str = "namespace";
const ROOT_COL: &str = "root";
const RECURSIVE_COL: &str = "recursive";
const PARENT_COL: &str = "parent";
const PATH_COL: &str = "path";
const NAME_COL: &str = "name";
const BLOB_COL: &str = "blob";
const IS_FILE_COL: &str = "is_file";
const SIZE_COL: &str = "size";
const CONTENT_TYPE_COL: &str = "content_type";
const LAST_MODIFIED_COL: &str = "last_modified";
const ORDERED_COLUMNS: &[&str] = &[
    URI_COL,
    NAMESPACE_COL,
    ROOT_COL,
    PARENT_COL,
    PATH_COL,
    NAME_COL,
    SIZE_COL,
    CONTENT_TYPE_COL,
    LAST_MODIFIED_COL,
];
const BOOLEAN_COLUMNS: &[&str] = &[
    RECURSIVE_COL,
    IS_FILE_COL,
];

// At this point, these determine global batch sizes for read operations
// New batches will start either every 1000 rows, or every 10 MB of blob file if they're projected,
// whichever comes first
const BATCH_SIZE_THRESHOLD: u64 = 100 * 1024 * 1024; // e.g., 10 MB
const ROW_COUNT_THRESHOLD: usize = 1000;


#[derive(Debug)]
pub enum OpenDALExecError {
    OpenDALErr(String),
    PoolErr(String),
    ArrowErr(String),
    InvalidSchema(String),
    OverwriteError(String),
    SendErr(String),
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

impl<T: Debug> From<SendError<T>> for OpenDALExecError {
    fn from(err: SendError<T>) -> Self {
        Self::SendErr(format!("{:?}",err))
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
            OpenDALExecError::SendErr(inner) => write!(f,"{}",inner),
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
    pub schema: SchemaRef,
}

impl<T> OpenDALDataSourceInner<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn new(bldr: T) -> Result<Self,OpenDALExecError> {
        let manager = OpenDALManager::new(bldr);
        let pool = managed::Pool::builder(manager).build()?;
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new(URI_COL, DataType::Utf8, true),
            Field::new(NAMESPACE_COL, DataType::Utf8, true),
            Field::new(ROOT_COL, DataType::Utf8, true),
            Field::new(RECURSIVE_COL, DataType::Boolean, true),
            Field::new(PARENT_COL, DataType::Utf8, true),
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
    pub fn new(bldr: T) -> Result<Self,OpenDALExecError> {
        Ok(Self {
            inner: Arc::new(OpenDALDataSourceInner::new(bldr)?),
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
    filters: Vec<Expr>,
}

impl<T> OpenDALExec<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub fn try_new(projections: Option<&Vec<usize>>, db: &OpenDALDataSource<T>, filters: &[Expr], limit: Option<usize>) -> Result<Self,DataFusionError> {
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
            filters: filters.to_vec(),
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
        let limit = self.limit.clone();
        let filter_refs = self.filters.iter().collect::<Vec<&Expr>>();
        // If there is an aggregate or something that doesn't project any columns, project primary
        // key
        let p_sch = if self.projected_schema.fields().len() == 0 {
            SchemaRef::new(Schema::new(vec![
                Field::new(PATH_COL, DataType::Utf8, false),
            ]))
        } else {
            Arc::clone(&self.projected_schema)
        };
        let mut conditions: HashMap<String,Vec<(ExprOp,ScalarValue)>> = HashMap::new();
        for (col,op,literal) in extract_simple_binary_filters(&filter_refs).into_iter().filter_map(|entry| entry).collect::<Vec<(String,ExprOp,ScalarValue)>>() {
            conditions.entry(col).and_modify(|vec: &mut Vec<(ExprOp, ScalarValue)> | vec.push((op,literal.clone()))).or_insert(vec![(op,literal)]);
        }

//      let mut fsize: Option<u64> = None;
//      let mut recursive = false;
//      let mut rel_path = "/";
//      let mut name: Option<String> = None;

        builder.spawn(async move {
            // OpenDAL Operator to interact with filesystem
            let op = pool
                .get()
                .await
                .map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;

            let op_info = op.info();
            let root: String = op_info.root();
            let uri: String = format!("{}://",op_info.scheme());
            let namespace: Option<String> = Some(op_info.name()).filter(|s| !s.is_empty());
            let mut recursive: Option<bool> = None;
            let mut input_path: Option<String> = None;
            let mut input_parent: Option<String> = None;
            if let Some(vec) = conditions.get(URI_COL) {
                for (op,literal) in vec {
                    // If URI doesn't match, we're in the wrong place
                    if !eval_simple_expr(uri.clone().into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {
                        return Ok(());
                    }
                }
            }

            if let Some(vec) = conditions.get(ROOT_COL) {
                for (op,literal) in vec {
                    // If root doesn't match, we're in the wrong place
                    if !eval_simple_expr(root.clone().into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {
                        return Ok(());
                    }
                }
            }

            if let Some(vec) = conditions.get(NAMESPACE_COL) {
                for (op,literal) in vec {
                    // If namespace doesn't match, we're in the wrong place
                    if !eval_simple_expr(ScalarValue::Utf8(namespace.clone()),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {
                        return Ok(());
                    }
                }
            }

            if let Some(vec) = conditions.get(RECURSIVE_COL) {
                for (op,literal) in vec {
                    // If there are multiple recursive conditions and they don't match, quit
                    if let Some(rec) = recursive {
                        if !eval_simple_expr(rec.into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {
                            return Ok(());
                        }
                    // I want to take recursive as an input parameter, take the first provided one
                    } else if let ScalarValue::Boolean(opt_bool) = literal {
                        recursive = *opt_bool;
                    } else {
                        return Err(Into::<DataFusionError>::into(OpenDALExecError::InvalidSchema(format!("Cannot evaluate `recursive {} {}`",op,literal))));
                    }
                }
            }
            let rec = recursive.unwrap_or(false);

            // This sets the directory in which we'll start listing
            if let Some(vec) = conditions.get(PARENT_COL) {
                for (op,literal) in vec {
                    // If there are multiple input paths and they don't match, quit
                    if let Some(p) = input_parent.clone() {
                        if !eval_simple_expr(p.into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {
                            return Ok(());
                        }
                    // I want to take recursive as an input parameter, take the first provided one
                    } else {
                        if let ScalarValue::Utf8(val) = &literal {
                            input_parent = Some(val.clone().unwrap_or("/".to_string()));
                        } else {
                            input_parent = Some("/".to_string());
                        }
                    }
                }
            }

            if let Some(vec) = conditions.get(PATH_COL) {
                if input_parent.is_some() {
                    return Err(
                        Into::<DataFusionError>::into(
                            OpenDALExecError::InvalidSchema(
                                "Cannot filter on both `parent` and `path`, choose one"
                                .to_string()
                                )
                            )
                        );
                }
                for (op,literal) in vec {
                    // If there are multiple input paths and they don't match, quit
                    if let Some(p) = input_path.clone() {
                        if !eval_simple_expr(p.into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {
                            return Ok(());
                        }
                    } else {
                        if let ScalarValue::Utf8(val) = &literal {
                            let temp = val.clone().unwrap_or("/".to_string());
                            input_path = Some(
                                Path::new(&temp)
                                .parent()
                                .unwrap_or(Path::new("/"))
                                .to_str()
                                .unwrap_or("/")
                                .to_string()
                            );
                        } else {
                            input_path = Some("/".to_string());
                        }
                    }
                }
            }

            let mut lister = {
                let mut builder = match (input_parent,input_path) {
                    (Some(p),None) => op.lister_with(&format!("{}/",&p)),
                    (None,Some(p)) => op.lister_with(&format!("{}/",&p)),
                    _ => op.lister_with("/"),
                };
                if let Some(r) = recursive {
                    builder = builder.recursive(r);
                }
                if let Some(l) = limit {
                    builder = builder.limit(l);
                }
                builder.await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?
            };

            // These correspond to columns
            let mut path_array = Vec::new();
            let mut parent_array = Vec::new();
            let mut root_array = Vec::new();
            let mut namespace_array = Vec::new();
            let mut uri_array = Vec::new();
            let mut recursive_array = Vec::new();
            let mut name_array = Vec::new();
            let mut blob_array = Vec::new();
            let mut file_array = Vec::new();
            let mut size_array = Vec::new();
            let mut content_array = Vec::new();
            let mut last_modified_array = Vec::new();

            let mut accumulated_size = 0u64;
            let mut row_count = 0usize;
            let include_blob = p_sch.fields.find(BLOB_COL).is_some();

            'batch: while let Some(entry) = lister.try_next().await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))? {
                // If it's not something we know about, don't process it
                if entry.metadata().mode() == EntryMode::Unknown {
                    continue;
                }
                // collect metadata
                let name = entry.name().to_string();
                let (path, meta) = entry.into_parts();
                let meta2 = op.stat(&path).await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;
                let is_file = meta.is_file();
                let sz = meta2.content_length();
                let con_type = meta2.content_type().map(str::to_string);
                let last_mod = meta2.last_modified().and_then(|t| t.timestamp_nanos_opt());
                let mut parent = Path::new(&path).parent().unwrap_or(Path::new("/")).to_str().unwrap_or("/").to_string();
                if parent != "/" {
                    parent.insert_str(0,"/");
                }

                // Filters
                if let Some(vec) = conditions.get(PATH_COL) {
                    for (op,literal) in vec {
                        if !eval_simple_expr(path.clone().into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {continue 'batch;}
                    }
                }
                if let Some(vec) = conditions.get(NAME_COL) {
                    for (op,literal) in vec {
                        if !eval_simple_expr(name.clone().into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {continue 'batch;}
                    }
                }
                if let Some(vec) = conditions.get(IS_FILE_COL) {
                    for (op,literal) in vec {
                        if !eval_simple_expr(is_file.into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {continue 'batch;}
                    }
                }
                if let Some(vec) = conditions.get(SIZE_COL) {
                    for (op,literal) in vec {
                        if !eval_simple_expr(sz.into(),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {continue 'batch;}
                    }
                }
                if let Some(vec) = conditions.get(CONTENT_TYPE_COL) {
                    for (op,literal) in vec {
                        if !eval_simple_expr(ScalarValue::Utf8(con_type.clone()),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {continue 'batch;}
                    }
                }
                if let Some(vec) = conditions.get(LAST_MODIFIED_COL) {
                    for (op,literal) in vec {
                        if !eval_simple_expr(ScalarValue::TimestampNanosecond(last_mod.clone(),None),*op,literal.clone()).map_err(Into::<DataFusionError>::into)? {continue 'batch;}
                    }
                }

                // Projections
                if p_sch.fields.find(URI_COL).is_some() {
                    uri_array.push(uri.clone());
                }
                if p_sch.fields.find(NAMESPACE_COL).is_some() {
                    namespace_array.push(namespace.clone());
                }
                if p_sch.fields.find(ROOT_COL).is_some() {
                    root_array.push(root.clone());
                }
                if p_sch.fields.find(RECURSIVE_COL).is_some() {
                    recursive_array.push(rec);
                }
                if p_sch.fields.find(PARENT_COL).is_some() {
                    parent_array.push(parent);
                }
                if p_sch.fields.find(NAME_COL).is_some() {
                    name_array.push(name);
                }
                if p_sch.fields.find(PATH_COL).is_some() {
                    path_array.push(path.clone());
                }
                if p_sch.fields.find(IS_FILE_COL).is_some() {
                    file_array.push(is_file);
                }
                if p_sch.fields.find(SIZE_COL).is_some() {
                    size_array.push(sz);
                }
                if p_sch.fields.find(CONTENT_TYPE_COL).is_some() {
                    content_array.push(con_type);
                }
                if p_sch.fields.find(LAST_MODIFIED_COL).is_some() {
                    last_modified_array.push(last_mod);
                }
                if include_blob && is_file {
                    blob_array.push(Some(op.read(&path).await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?.to_vec()));
                } else {
                    blob_array.push(None);
                }

                // Increment batch counters
                accumulated_size += sz;
                row_count += 1;

                let should_flush = if include_blob {
                    accumulated_size >= BATCH_SIZE_THRESHOLD || row_count >= ROW_COUNT_THRESHOLD
                } else {
                    row_count >= ROW_COUNT_THRESHOLD
                };

                // flush if we exceed threshold
                if should_flush {
                    let batch = build_batch(
                        &p_sch,
                        &uri_array,
                        &namespace_array,
                        &root_array,
                        &recursive_array,
                        &parent_array,
                        &path_array,
                        &name_array,
                        &blob_array,
                        &file_array,
                        &size_array,
                        &content_array,
                        &last_modified_array,
                    )?;

                    tx.send(Ok(batch)).await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;

                    // reset buffers
                    uri_array.clear();
                    namespace_array.clear();
                    root_array.clear();
                    recursive_array.clear();
                    parent_array.clear();
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
                    &uri_array,
                    &namespace_array,
                    &root_array,
                    &recursive_array,
                    &parent_array,
                    &path_array,
                    &name_array,
                    &blob_array,
                    &file_array,
                    &size_array,
                    &content_array,
                    &last_modified_array,
                )?;
                tx.send(Ok(batch)).await.map_err(|e| Into::<DataFusionError>::into(OpenDALExecError::from(e)))?;
            }

            Ok(())
        });

        Ok(builder.build())
    }
}

fn build_batch(
    schema: &SchemaRef,
    uris: &[String],
    namespaces: &[Option<String>],
    roots: &[String],
    recursives: &[bool],
    parents: &[String],
    paths: &[String],
    names: &[String],
    blobs: &[Option<Vec<u8>>],
    is_files: &[bool],
    sizes: &[u64],
    content_types: &[Option<String>],
    last_modified: &[Option<i64>],
) -> Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    if schema.fields.find(URI_COL).is_some() {
        columns.push(Arc::new(StringArray::from(uris.to_vec())));
    }
    if schema.fields.find(NAMESPACE_COL).is_some() {
        columns.push(Arc::new(StringArray::from(namespaces.to_vec())));
    }
    if schema.fields.find(ROOT_COL).is_some() {
        columns.push(Arc::new(StringArray::from(roots.to_vec())));
    }
    if schema.fields.find(RECURSIVE_COL).is_some() {
        columns.push(Arc::new(BooleanArray::from(recursives.to_vec())));
    }
    if schema.fields.find(PARENT_COL).is_some() {
        columns.push(Arc::new(StringArray::from(parents.to_vec())));
    }
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

pub fn extract_simple_binary_filters(exprs: &[&Expr]) -> Vec<Option<(String, ExprOp, ScalarValue)>> {
    exprs.iter().map(|expr| {
        // Only handle BinaryExpr nodes
        match expr { 
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                match (&**left, &**right, matches!(&**left, Expr::Column(_))) {
                    (Expr::Column(col), Expr::Literal(value,_),val) |
                    (Expr::Literal(value,_), Expr::Column(col),val) => {
                        if ORDERED_COLUMNS.contains(&col.name()) 
                                && matches!(op, ExprOp::Eq |ExprOp::NotEq |ExprOp::Lt |ExprOp::LtEq |ExprOp::Gt |ExprOp::GtEq) { 
                            if val {
                                Some((col.name.clone(), op.clone(), value.clone()))
                            } else {
                                Some((col.name.clone(), op.swap()?, value.clone()))
                            }
                        } else {
                            None
                        }
                    },
                    _ => None, // anything else is too complex
                }
            },
            Expr::Column(col) => {
                if BOOLEAN_COLUMNS.contains(&col.name()) {
                    Some((col.name.clone(), ExprOp::Eq, ScalarValue::Boolean(Some(true))))
                } else {
                    None
                }
            },
            Expr::Not(expr) => {
                if let Expr::Column(col) = &**expr {
                    if BOOLEAN_COLUMNS.contains(&col.name()) {
                        Some((col.name.clone(), ExprOp::Eq, ScalarValue::Boolean(Some(false))))
                    } else {
                        None
                    }
                } else {
                    None
                }
            },
            _ => None,
        }
    }).collect()
}

fn eval_simple_expr(left: ScalarValue, op: ExprOp, right: ScalarValue) -> Result<bool,OpenDALExecError> {
    match op {
        ExprOp::Eq => Ok(left.eq(&right)),
        ExprOp::NotEq => Ok(!left.eq(&right)),
        ExprOp::Gt => Ok(left.gt(&right)),
        ExprOp::GtEq => Ok(left.gt(&right) || left.eq(&right)),
        ExprOp::Lt => Ok(left.lt(&right)),
        ExprOp::LtEq => Ok(left.lt(&right) || left.eq(&right)),
        _ => Err(OpenDALExecError::InvalidSchema(format!("Expr `{} {} {}` cannot be evaluated",left,op,right))),
    }
}
