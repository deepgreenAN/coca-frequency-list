use crate::{Columns, CustomError, Error, MatchType, SheetType};

use datafusion::logical_expr;
use datafusion::prelude::{DataFrame, Expr};

/// cliのクエリのベース
pub fn simple_query(
    mut df: DataFrame,
    sheet_type: SheetType,
    words_and_match: Option<(&[String], MatchType)>,
    pos: Option<&str>,
    sorted_column: Option<&str>,
    skip: Option<usize>,
    limit: Option<usize>,
    additional_columns: Option<&[String]>,
) -> Result<DataFrame, Error> {
    let mut columns = match sheet_type {
        SheetType::First => crate::columns!["rank", "lemma", "freq"],
        SheetType::Second => crate::columns!["rank", "lemma"],
        SheetType::Third => Columns::all(),
        SheetType::Fourth => crate::columns!["rank", "word", "freq", "#texts"],
    };

    // where句を記述する
    let mut where_expr: Option<Expr> = None;

    // words_and_match
    if let Some((words, match_type)) = words_and_match {
        // 検索したいカラム
        let column_name = match sheet_type {
            SheetType::First | SheetType::Second => "lemma",
            SheetType::Third | SheetType::Fourth => "word",
        };

        let words_expr = match match_type {
            MatchType::All => logical_expr::in_list(
                logical_expr::col(column_name),
                words
                    .into_iter()
                    .map(|word| logical_expr::lit(word))
                    .collect(),
                false,
            ),
            MatchType::Prefix => words
                .into_iter()
                .map(|word| {
                    logical_expr::starts_with(
                        logical_expr::col(column_name),
                        logical_expr::lit(word),
                    )
                })
                .reduce(|acc, expr| acc.or(expr))
                .unwrap(),
            MatchType::Suffix => words
                .into_iter()
                .map(|word| {
                    logical_expr::ends_with(logical_expr::col(column_name), logical_expr::lit(word))
                })
                .reduce(|acc, expr| acc.or(expr))
                .unwrap(),
        };

        // where_exprの更新
        match where_expr {
            Some(expr) => where_expr = Some(expr.and(words_expr)),
            None => {
                where_expr = Some(words_expr);
            }
        };
    }

    // pos
    if let Some(pos) = pos {
        // まずPoSがあるか確認
        if !df.schema().has_column_with_unqualified_name("PoS") {
            Err(Error::ArgError(
                CustomError::msg("Invalid sheet type for specifying part of speech(PoS).").into(),
            ))?;
        }

        let pos_expr = logical_expr::col(r#""PoS""#).eq(logical_expr::lit(pos));

        // where_exprの更新
        match where_expr {
            Some(expr) => where_expr = Some(expr.and(pos_expr)),
            None => {
                where_expr = Some(pos_expr);
            }
        };
    }

    // where句の追加
    if let Some(where_expr) = where_expr {
        df = df.filter(where_expr)?;
    }

    // sorted_column
    if let Some(sorted_column) = sorted_column {
        // ソートしたいカラムがあるか確認
        if !df.schema().has_column_with_unqualified_name(sorted_column) {
            Err(Error::ArgError(
                CustomError::msg("Invalid column for sorting in the specified sheet.").into(),
            ))?;
        }

        columns.insert(sorted_column.to_string());

        df = df.sort(vec![logical_expr::col(sorted_column).sort(false, false)])?;
    }

    // skip and limit
    if skip.is_some() || limit.is_some() {
        df = df.limit(skip.unwrap_or(0), limit)?;
    }

    // columns
    if let Some(additional_columns) = additional_columns {
        for column in additional_columns.into_iter() {
            // 追加するカラムがあるか確認
            if !df.schema().has_column_with_unqualified_name(&column) {
                Err(Error::ArgError(
                    CustomError::msg("Invalid column for select in the specified sheet.").into(),
                ))?;
            }

            columns.insert(column.to_string());
        }
    }
    if let Columns::List(list) = columns {
        df = df.select(
            list.into_iter()
                .map(|column| logical_expr::col(column))
                .collect::<Vec<_>>(),
        )?;
    }

    Ok(df)
}
