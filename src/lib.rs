mod error;
pub mod query;
pub use error::{CustomError, Error};

#[derive(Debug, Clone, Copy)]
pub enum SheetType {
    First,
    Second,
    Third,
    Fourth,
}

impl SheetType {
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
        match &self {
            Self::First => "lemmas",
            Self::Second => "subgenres",
            Self::Third => "wordForms",
            Self::Fourth => "forms",
        }
        .to_string()
    }
}

impl TryFrom<usize> for SheetType {
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

#[derive(Debug, Clone, Copy)]
pub enum MatchType {
    All,
    Prefix,
    Suffix,
}

#[derive(Debug, Clone)]
pub enum Columns {
    All,
    List(indexmap::IndexSet<String>),
}

impl Columns {
    pub fn all() -> Self {
        Columns::All
    }
    pub fn list() -> Self {
        Columns::List(indexmap::IndexSet::new())
    }
    pub fn insert(&mut self, column: String) -> bool {
        match self {
            Columns::All => false,
            Columns::List(set) => set.insert(column),
        }
    }
}

#[macro_export]
macro_rules! columns {
    ($($column:expr),*) => {
        {
            let mut set = $crate::Columns::list();
            $(
                let _ = set.insert($column.to_string());
            )*
            set
        }
    };
}
