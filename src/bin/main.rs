use coca_frequency_list::{query::simple_query, CustomError, Error, MatchType, SheetType};

use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;

/// query the coca frequency list
#[derive(Parser, Debug)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands,
}

/// サブコマンド間で共通するコマンドライン引数
#[derive(Args, Debug)]
struct Common {
    /// path for saving the query result as csv file
    #[arg(long)]
    dist_path: Option<PathBuf>,

    /// skip number of rows
    #[arg(long)]
    skip: Option<usize>,

    /// limit row number of query result
    #[arg(long)]
    limit: Option<usize>,
}

/// サブコマンドの定義
#[derive(Subcommand, Debug)]
enum Commands {
    /// query with sql
    Sql {
        /// sql statement for query
        sql: String,

        /// sheet numbers of frequency data
        #[arg(long)]
        sheets: Option<Vec<usize>>,

        #[command(flatten)]
        common: Common,
    },
    /// query via cli arguments
    Query {
        /// search terms
        #[arg(long)]
        words: Option<String>,

        /// search terms with specified prefix
        #[arg(long)]
        prefix: bool,

        /// search terms with specified prefix
        #[arg(long)]
        suffix: bool,

        /// filtering by part of speech(pos)
        #[arg(long)]
        pos: Option<String>,

        /// column name for sorting
        #[arg(long)]
        sorted: Option<String>,

        /// sheet number of frequency data
        #[arg(long)]
        sheet: Option<usize>,

        /// additional columns
        #[arg(long)]
        columns: Option<Vec<String>>,

        #[command(flatten)]
        common: Common,
    },
}

/// データソースの登録
async fn register_data(ctx: &SessionContext, sheet_type: SheetType) -> Result<(), Error> {
    let data_path = format!("./data/{}", sheet_type.file_name());
    if !PathBuf::from(data_path.clone()).is_file() {
        Err(Error::IOError(CustomError::msg(
            r#"
The data source files cannot be found. Please download xlsx file from the official website: https://www.wordfrequency.info/samples.asp and locate it as "./data/wordFrequency.xlsx". And then run command `just build`.
        "#,
        ).into()))?;
    }
    ctx.register_csv(&sheet_type.table_name(), &data_path, Default::default())
        .await?;

    Ok(())
}

/// sqlコマンド
async fn sql_command(
    ctx: &SessionContext,
    sql: &str,
    skip: Option<usize>,
    limit: Option<usize>,
    dist_path: Option<&Path>,
) -> Result<(), Error> {
    let mut df = ctx.sql(sql).await?;

    if skip.is_some() || limit.is_some() {
        df = df.limit(skip.unwrap_or(0), limit)?;
    }

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

/// queryコマンド
async fn query_command(
    ctx: &SessionContext,
    sheet_type: SheetType,
    words: Option<&[String]>,
    prefix: bool,
    suffix: bool,
    pos: Option<&str>,
    sorted_column: Option<&str>,
    skip: Option<usize>,
    limit: Option<usize>,
    columns: Option<&[String]>,
    dist_path: Option<&Path>,
) -> Result<(), Error> {
    let words_and_match = match (words, prefix, suffix) {
        (Some(words), true, false) => Some((words, MatchType::Prefix)),
        (Some(words), false, true) => Some((words, MatchType::Suffix)),
        (Some(words), false, false) => Some((words, MatchType::All)),
        (Some(_), true, true) => {
            return Err(Error::ArgError(
                CustomError::msg("prefix and suffix cannot be specified at the same time.").into(),
            ));
        }
        (None, false, false) => None,
        (None, _, _) => {
            return Err(Error::ArgError(
                CustomError::msg("You can specify prefix or suffix with search words.").into(),
            ));
        }
    };

    let csv_path = format!("./data/{}", sheet_type.file_name());

    if !PathBuf::from(csv_path.clone()).is_file() {
        Err(Error::IOError(CustomError::msg(
            r#"
The data source files cannot be found. Please download xlsx file from the official website: https://www.wordfrequency.info/samples.asp and locate it as "./data/wordFrequency.xlsx". And then run command `just build`.
        "#,
        ).into()))?;
    }

    let df = ctx.read_csv(&csv_path, Default::default()).await?;

    let df = simple_query(
        df,
        sheet_type,
        words_and_match,
        pos,
        sorted_column,
        skip,
        limit,
        columns,
    )?;

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
async fn main() -> Result<(), Error> {
    let CliArgs { command } = CliArgs::parse();

    let ctx = SessionContext::new();

    match command {
        Commands::Sql {
            sql,
            sheets,
            common:
                Common {
                    dist_path,
                    skip,
                    limit,
                },
        } => {
            for sheet_number in sheets.unwrap_or(vec![1]) {
                let sheet_type = TryInto::<SheetType>::try_into(sheet_number)
                    .map_err(|e| Error::ArgError(e.into()))?;

                register_data(&ctx, sheet_type).await?;
            }

            sql_command(&ctx, &sql, skip, limit, dist_path.as_deref()).await?;
        }
        Commands::Query {
            words,
            prefix,
            suffix,
            pos,
            sheet,
            sorted,
            columns,
            common:
                Common {
                    dist_path,
                    skip,
                    limit,
                },
        } => {
            let sheet_type = TryInto::<SheetType>::try_into(sheet.unwrap_or(1))
                .map_err(|e| Error::ArgError(e.into()))?;

            let words = words.map(|words| {
                words
                    .split(",")
                    .map(|word| word.to_owned())
                    .collect::<Vec<_>>()
            });

            register_data(&ctx, sheet_type).await?;

            query_command(
                &ctx,
                sheet_type,
                words.as_deref(),
                prefix,
                suffix,
                pos.as_deref(),
                sorted.as_deref(),
                skip,
                limit,
                columns.as_deref(),
                dist_path.as_deref(),
            )
            .await?;
        }
    }

    Ok(())
}
