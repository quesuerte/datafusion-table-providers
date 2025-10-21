use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::expr::Expr;
use datafusion::physical_plan::{project_schema,ExecutionPlan};
use std::sync::Arc;
use std::any::Any;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion::error::DataFusionError;
use opendal::Configurator;
use std::fmt::Debug;

mod executor;
use crate::opendal::executor::OpenDALExec;
pub use crate::opendal::executor::OpenDALDataSource;

impl<T> OpenDALExec<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: OpenDALDataSource<T>) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        Self {
            db,
            projected_schema,
        }
    }
}

impl<T> OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>,DataFusionError> {
        Ok(Arc::new(OpenDALExec::new(projections, schema, self.clone())))
    }
}

#[async_trait]
impl<T> TableProvider for OpenDALDataSource<T>
where
    T: Configurator + Clone + Send + Sync + 'static + Debug,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
        ]))
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
        return self
            .create_physical_plan(projection, self.schema(), filters, limit)
            .await;
    }
}
