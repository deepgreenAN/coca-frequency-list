mod error;
pub use error::{CustomError, Error};

#[derive(Debug, Clone, Copy)]
pub enum DataSourceType {
    First,
    Second,
    Third,
    Fourth,
}

impl DataSourceType {
    pub fn file_name(&self) -> String {
        match self {
            Self::First => "wordFrequencyFirst.csv",
            Self::Second => "wordFrequencySecond.csv",
            Self::Third => "wordFrequencyThird.csv",
            Self::Fourth => "wordFrequencyFourth.csv",
        }
        .to_string()
    }
    pub fn sheet_name(&self) -> String {
        match self {
            Self::First => "1 lemmas",
            Self::Second => "2 subgenres",
            Self::Third => "3 wordForms",
            Self::Fourth => "4 forms (219k)",
        }
        .to_string()
    }
    pub fn table_name(&self) -> String {
        match self {
            Self::First => "lemmas",
            Self::Second => "subgenres",
            Self::Third => "wordForms",
            Self::Fourth => "forms",
        }
        .to_string()
    }
}

impl TryFrom<usize> for DataSourceType {
    type Error = CustomError;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::First),
            2 => Ok(Self::Second),
            3 => Ok(Self::Third),
            4 => Ok(Self::Fourth),
            _ => Err(CustomError::msg(
                "Invalid usize for converting into DataSourceType.",
            )),
        }
    }
}
