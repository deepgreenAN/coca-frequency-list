use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    ctx.register_csv("lemmas_60k", "./data/lemmas_60k.csv", Default::default())
        .await?;

    let df = ctx
        .sql(r#"SELECT * FROM lemmas_60k WHERE "PoS" = 'v'"#)
        .await?;

    println!("{:#?}", df.schema());

    df.show_limit(1000).await?;

    Ok(())
}
