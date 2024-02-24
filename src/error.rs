use core::fmt;
use std::fmt::Display;

/// CLIアプリケーションのエラー．標準エラー出力に表示する．
#[derive(thiserror::Error)]
pub enum Error {
    /// コマンドライン引数に関するエラー．
    #[error("ArgError: Error related on cli arguments occurred. {0}")]
    ArgError(Box<dyn std::error::Error>),

    /// ファイルIOに関するエラー．
    #[error("IOError: Error related on file-IO occurred. {0}")]
    IOError(Box<dyn std::error::Error>),

    /// xlsxファイルの取り扱いに関するエラー．
    #[error("XlsxError: Error related on handling xlsx file occurred. {0}")]
    XlsxError(Box<dyn std::error::Error>),

    /// データフレームに関するエラー．
    #[error("DataFrameError: Error related on dataframe occurred. {0}")]
    DataFrameError(Box<dyn std::error::Error>),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <Self as std::fmt::Display>::fmt(&self, f)
    }
}

/// 外部には直接公開しないカスタムエラー．
#[derive(Debug)]
pub struct CustomError {
    msg: String,
    inner: Option<Box<dyn std::error::Error>>,
}

impl CustomError {
    pub fn msg<D: Display>(s: D) -> Self {
        Self {
            msg: format!("{s}"),
            inner: None,
        }
    }
    pub fn new<D: Display, E: std::error::Error + 'static>(s: D, inner: E) -> Self {
        Self {
            msg: format!("{s}"),
            inner: Some(Box::new(inner) as Box<dyn std::error::Error>),
        }
    }
}

impl Display for CustomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Some(inner) => write!(f, "msg: {}, inner: {}", &self.msg, inner),
            None => write!(f, "{}", self.msg),
        }
    }
}

impl std::error::Error for CustomError {}

// -------------------------------------------------------------------------------------------------
// ベースの変換

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IOError(value.into())
    }
}

impl From<calamine::Error> for Error {
    fn from(value: calamine::Error) -> Self {
        Error::XlsxError(value.into())
    }
}

impl From<datafusion::error::DataFusionError> for Error {
    fn from(value: datafusion::error::DataFusionError) -> Self {
        Error::DataFrameError(value.into())
    }
}
