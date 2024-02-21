use coca_frequency_list::{CustomError, DataSourceType, Error};

use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;

/// コマンドライン引数の定義
#[derive(Parser, Debug)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands,
}

/// サブコマンド間で共通するコマンドライン引数
#[derive(Args, Debug)]
struct Common {
    #[arg(long)]
    source_type: Option<usize>,

    #[arg(long)]
    dist_path: Option<PathBuf>,
}

/// サブコマンドの定義
#[derive(Subcommand, Debug)]
enum Commands {
    Select {
        #[arg(long)]
        sql: String,

        #[arg(long)]
        start: Option<usize>,

        #[arg(long)]
        end: Option<usize>,

        #[command(flatten)]
        common: Common,
    },
    Search {
        words: Vec<String>,

        #[command(flatten)]
        common: Common,
    },
}

/// データソースの登録
async fn register_data(ctx: &SessionContext, source_type: DataSourceType) -> Result<(), Error> {
    let data_path = format!("./data/{}", source_type.file_name());
    if !PathBuf::from(data_path.clone()).is_file() {
        Err(Error::IOError(CustomError::msg(
            r#"
The data source files cannot be found. Please download xlsx file from the official website: https://www.wordfrequency.info/samples.asp and locate it as "./data/wordFrequency.xlsx". And then run command `just build`.
        "#,
        ).into()))?;
    }
    ctx.register_csv(&source_type.table_name(), &data_path, Default::default())
        .await?;

    Ok(())
}

/// selectコマンド
async fn select(
    ctx: &SessionContext,
    sql: &str,
    start: Option<usize>,
    end: Option<usize>,
    dist_path: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let df = ctx.sql(sql).await?.limit(start.unwrap_or(0), end)?;

    match dist_path {
        Some(path) => {
            let opt = DataFrameWriteOptions::new().with_single_file_output(true);

            df.write_csv(path.to_str().unwrap(), opt, None).await?;
        }
        None => {
            df.show().await?;
        }
    }
    Ok(())
}

/// searchサブコマンド
async fn search(
    ctx: &SessionContext,
    source_type: DataSourceType,
    words: &[String],
    dist_path: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let words = words
        .iter()
        .map(|word| format!(r#"'{word}'"#))
        .collect::<Vec<String>>();

    let sql = match source_type {
        t @ DataSourceType::First | t @ DataSourceType::Second => {
            format!(
                r#"
SELECT * from "{}" WHERE lemma in ({})
            "#,
                t.table_name(),
                words.join(",")
            )
        }
        t @ DataSourceType::Third | t @ DataSourceType::Fourth => {
            format!(
                r#"
SELECT * from "{}" WHERE word in ({})
            "#,
                t.table_name(),
                words.join(",")
            )
        }
    };

    let df = ctx.sql(&sql).await?;

    match dist_path {
        Some(path) => {
            let opt = DataFrameWriteOptions::new().with_single_file_output(true);

            df.write_csv(path.to_str().unwrap(), opt, None).await?;
        }
        None => {
            df.show().await?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let CliArgs { command } = CliArgs::parse();

    let ctx = SessionContext::new();

    match command {
        Commands::Select {
            sql,
            start,
            end,
            common: Common {
                source_type,
                dist_path,
            },
        } => {
            let source_type = {
                let source_type_n = source_type.unwrap_or(1);
                TryInto::<DataSourceType>::try_into(source_type_n)
                    .map_err(|e| Error::ArgError(e.into()))?
            };

            register_data(&ctx, source_type).await?;
            select(&ctx, &sql, start, end, dist_path.as_deref()).await?;
        }
        Commands::Search {
            words,
            common: Common {
                source_type,
                dist_path,
            },
        } => {
            let source_type = {
                let source_type_n = source_type.unwrap_or(1);
                TryInto::<DataSourceType>::try_into(source_type_n)
                    .map_err(|e| Error::ArgError(e.into()))?
            };

            register_data(&ctx, source_type).await?;
            search(&ctx, source_type, &words, dist_path.as_deref()).await?;
        }
    }

    Ok(())
}
