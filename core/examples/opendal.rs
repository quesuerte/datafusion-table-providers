use datafusion::prelude::SessionContext;
use datafusion_table_providers::opendal::OpenDALDataSource;
use std::sync::Arc;
use opendal::services::FsConfig;

/// This example demonstrates how to:
/// 1. Create a PostgreSQL connection pool
/// 2. Create and use PostgresTableFactory to generate TableProvider
/// 3. Register TableProvider with DataFusion
/// 4. Use SQL queries to access PostgreSQL table data
///
/// Prerequisites:
/// Start a PostgreSQL server using Docker:
/// ```bash
/// docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
/// # Wait for the Postgres server to start
/// sleep 30
///
/// # Create a table and insert sample data
/// docker exec -i postgres psql -U postgres test_db <<EOF
/// CREATE TABLE companies (
///    id SERIAL PRIMARY KEY,
///    name VARCHAR(100)
/// );
///
/// INSERT INTO companies (name) VALUES ('Example Corp');
/// EOF
/// ```
#[tokio::main]
async fn main() {
    let mut cfg = FsConfig::default();
    cfg.root = Some("/".to_string());
    let table_factory = OpenDALDataSource::new(cfg,"/home/blinn/Downloads/".to_string()).unwrap();
    //let table_factory = OpenDALDataSource::new(Fs::default().root("/")).unwrap();

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Demonstrate direct table provider registration
    // This method registers the table in the default catalog
    // Here we register the PostgreSQL "companies" table as "companies_v2"
    ctx.register_table(
        "root_files",
        Arc::new(table_factory)
    )
    .expect("failed to register table");

    // Query Example 1: Query the renamed table through default catalog
    let df = ctx
        .sql("SELECT name, is_file, size, content_type FROM datafusion.public.root_files")
        .await
        .expect("select failed");
    match df.show().await {
        Ok(_val) => (),
        Err(err) => println!("Failed to retreive results: {}",err)
    };
}
