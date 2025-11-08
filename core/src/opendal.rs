use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType, sink::DataSinkExec};
use datafusion::logical_expr::{dml::InsertOp,expr::Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use std::sync::Arc;
use std::any::Any;

use opendal::Configurator;
use std::fmt::Debug;

mod executor;
use crate::opendal::executor::{OpenDALExec,OpenDALDataSink,extract_simple_binary_filters};
pub use crate::opendal::executor::OpenDALDataSource;

#[async_trait]
impl<T> TableProvider for OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>,DataFusionError> {
        Ok(Arc::new(OpenDALExec::try_new(projection, self, filters, limit)?))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(OpenDALDataSink::new(
                self,
                op,
            )),
            None,
        )) as _)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let mut rtn = Vec::new();
        for filter in extract_simple_binary_filters(filters) {
            if filter.is_some() {
                rtn.push(TableProviderFilterPushDown::Exact)
            } else {
                rtn.push(TableProviderFilterPushDown::Unsupported)
            }
        }
        Ok(rtn)
    }
}
