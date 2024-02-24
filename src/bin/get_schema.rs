use coca_frequency_list::SheetType;

use clap::Parser;
use datafusion::prelude::SessionContext;

async fn print_schema(
    ctx: &SessionContext,
    sheet_type: SheetType,
) -> Result<(), Box<dyn std::error::Error>> {
    let df = ctx
        .read_csv(
            &format!("./data/{}", sheet_type.file_name()),
            Default::default(),
        )
        .await?;

    println!("{:#?}", df.schema());

    Ok(())
}

#[derive(Parser, Debug)]
struct CliArg {
    #[arg(long)]
    source_type: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let CliArg { source_type } = CliArg::parse();
    let sheet_type: SheetType = source_type.unwrap_or(1).try_into()?;

    let ctx = SessionContext::new();

    print_schema(&ctx, sheet_type).await?;

    Ok(())
}
