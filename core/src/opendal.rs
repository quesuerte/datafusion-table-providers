use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::expr::Expr;
use datafusion::physical_plan::project_schema;

impl CustomExec {
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: CustomDataSource) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        Self {
            db,
            projected_schema,
        }
    }
}

impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self
            .create_physical_plan(projection, self.schema(), filters, limit)
            .await;
    }
}
