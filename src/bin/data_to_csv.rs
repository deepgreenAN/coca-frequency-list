use coca_frequency_list::Error;

use std::io::Write;
use std::path::PathBuf;

use calamine::{Data, Range, Reader};
use clap::Parser;

/// xlsxのRangeデータをcsvWriterに書き込む
fn write_range<W: Write>(writer: &mut W, range: &Range<Data>) -> Result<(), Error> {
    let width = range.get_size().1 - 1;

    for row in range.rows() {
        for (i, data) in row.iter().enumerate() {
            match data {
                Data::Empty => Ok(()),
                Data::String(s) | Data::DateTimeIso(s) | Data::DurationIso(s) => {
                    write!(writer, "{}", s)
                }
                Data::Float(f) => write!(writer, "{:.2}", f),
                Data::DateTime(d) => write!(writer, "{}", d),
                Data::Int(i) => write!(writer, "{:.0}", i),
                Data::Error(e) => write!(writer, "{:?}", e),
                Data::Bool(b) => write!(writer, "{}", b),
            }?;

            if i != width {
                write!(writer, ",")?;
            }
        }
        write!(writer, "\r\n")?;
    }
    Ok(())
}

/// コマンドライン引数
#[derive(Parser, Debug)]
struct CliArgs {
    #[arg(long)]
    source: Option<PathBuf>,
}

fn main() -> Result<(), Error> {
    use coca_frequency_list::{CustomError, SheetType};

    use std::fs::File;
    use std::io::BufWriter;

    use calamine::open_workbook_auto;

    let CliArgs { source } = CliArgs::parse();

    let mut xl = {
        let source_path = if let Some(source_path) = source {
            if !source_path.is_file() {
                Err(Error::ArgError(
                    CustomError::msg(format!(
                        r#"
Specified xlsx source path might be wrong. path: {}
                "#,
                        source_path.display()
                    ))
                    .into(),
                ))?;
            }
            source_path
        } else {
            let default_path = PathBuf::from("./data/wordFrequency.xlsx");
            if !default_path.is_file() {
                Err(Error::IOError(CustomError::msg(r#"
The xlsx source cannot be found. Please download it from the official website: https://www.wordfrequency.info/samples.asp and locate it as "./data/wordFrequency.xlsx" or specify its location.
                "#).into()))?;
            }
            default_path
        };

        open_workbook_auto(source_path)?
    };

    {
        let range = xl.worksheet_range(&SheetType::First.sheet_name())?;
        let mut writer = BufWriter::new(File::create(PathBuf::from(format!(
            "./data/{}",
            SheetType::First.file_name()
        )))?);

        write_range(&mut writer, &range)?;
    }
    {
        let range = xl.worksheet_range(&SheetType::Second.sheet_name())?;
        let mut writer = BufWriter::new(File::create(PathBuf::from(format!(
            "./data/{}",
            SheetType::Second.file_name()
        )))?);

        write_range(&mut writer, &range)?;
    }
    {
        let range = xl.worksheet_range(&SheetType::Third.sheet_name())?;
        let mut writer = BufWriter::new(File::create(PathBuf::from(format!(
            "./data/{}",
            SheetType::Third.file_name()
        )))?);

        write_range(&mut writer, &range)?;
    }
    {
        let range = xl.worksheet_range(&SheetType::Fourth.sheet_name())?;
        let mut writer = BufWriter::new(File::create(PathBuf::from(format!(
            "./data/{}",
            SheetType::Fourth.file_name()
        )))?);

        write_range(&mut writer, &range)?;
    }

    Ok(())
}
