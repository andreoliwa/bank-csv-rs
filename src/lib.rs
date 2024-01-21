use chrono::NaiveDate;
use csv::StringRecord;
use polars::prelude::*;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Display;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

const DOUBLE_QUOTE: char = '"';
pub const NUM_COLUMNS: usize = 5;
const PAYPAL_COLUMNS: [&str; NUM_COLUMNS] = ["Date", "Time", "TimeZone", "Name", "Type"];
const PAYPAL_COLUMNS_OLD: [&str; NUM_COLUMNS] =
    ["Date", "Time", "Time Zone", "Description", "Currency"];
const N26_COLUMNS: [&str; NUM_COLUMNS] = [
    "Date",
    "Payee",
    "Account number",
    "Transaction type",
    "Payment reference",
];

pub enum Source {
    N26,
    PayPal,
}

impl Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Source::N26 => "N26".to_string(),
            Source::PayPal => "PayPal".to_string(),
        };
        write!(f, "{}", str)
    }
}

pub fn detect_separator(file_path: &Path) -> io::Result<u8> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    if let Some(first_line) = reader.lines().next() {
        let line = first_line?;

        // Search for the separator character
        if line.contains(';') {
            Ok(b';')
        } else if line.contains(',') {
            Ok(b',')
        } else if line.contains('\t') {
            Ok(b'\t')
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "{}: No separator found in the first line",
                    file_path.display()
                ),
            ))
        }
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: Error reading the first line", file_path.display()),
        ))
    }
}

pub fn filter_data_frame(df: &DataFrame, upper_currency: String) -> (Source, DataFrame) {
    let schema = df.schema();
    let first_columns: Vec<&str> = schema
        .iter_names()
        .take(NUM_COLUMNS)
        .map(|field| field.as_str())
        .collect();

    let columns_to_select: [&str; NUM_COLUMNS];
    let source: Source;
    let lazy_frame: LazyFrame;
    let cloned_df = df.clone();

    // TODO: move these configs to separate structs or enums instead of "if" statements
    if first_columns == PAYPAL_COLUMNS {
        source = Source::PayPal;
        columns_to_select = ["Date", "Currency", "Gross", "Type", "Name"];
        lazy_frame = cloned_df
            .lazy()
            .filter(col("Currency").eq(lit(upper_currency.as_str())))
            .filter(col("Balance Impact").eq(lit("Debit")));
    } else if first_columns == PAYPAL_COLUMNS_OLD {
        source = Source::PayPal;
        columns_to_select = ["Date", "Currency", "Gross", "Description", "Name"];
        lazy_frame = cloned_df
            .lazy()
            .filter(col("Currency").eq(lit(upper_currency.as_str())))
            .filter(col("Description").neq(lit("General Currency Conversion")));
    } else if first_columns == N26_COLUMNS {
        source = Source::N26;
        let amount_column = if upper_currency == "EUR" {
            "Amount (EUR)"
        } else {
            "Amount (Foreign Currency)"
        };
        columns_to_select = [
            "Date",
            "Type Foreign Currency",
            amount_column,
            "Transaction type",
            "Payee",
        ];
        lazy_frame = cloned_df
            .lazy()
            .filter(col("Type Foreign Currency").eq(lit(upper_currency.as_str())))
    } else {
        panic!(
            "Unknown CSV format. These are the first columns: {:?}",
            first_columns
        );
    }

    (
        source,
        lazy_frame
            .select([cols(columns_to_select)])
            .collect()
            .unwrap(),
    )
}

#[derive(PartialEq, Eq)]
pub struct CsvOutputRow {
    pub date: NaiveDate,
    pub source: String,
    pub currency: String,
    pub amount: String,
    pub transaction_type: String,
    pub payee: String,
}

impl PartialOrd for CsvOutputRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CsvOutputRow {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.date.cmp(&other.date) {
            Ordering::Equal => match self.currency.cmp(&other.currency) {
                Ordering::Equal => match self.amount.cmp(&other.amount) {
                    Ordering::Equal => match self.transaction_type.cmp(&other.transaction_type) {
                        Ordering::Equal => self.payee.cmp(&other.payee),
                        other => other,
                    },
                    other => other,
                },
                other => other,
            },
            other => other,
        }
    }
}

impl fmt::Display for CsvOutputRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} [{}] {} {} paid to {} ({})",
            self.date, self.source, self.currency, self.amount, self.payee, self.transaction_type,
        )
    }
}

fn strip_quotes(s: String) -> String {
    s.strip_prefix(DOUBLE_QUOTE)
        .unwrap_or(s.as_str())
        .strip_suffix(DOUBLE_QUOTE)
        .unwrap_or(s.as_str())
        .to_string()
}

impl CsvOutputRow {
    pub fn new(
        date: NaiveDate,
        source: String,
        currency: String,
        amount: String,
        transaction_type: String,
        payee: String,
    ) -> Self {
        Self {
            date,
            source,
            currency: strip_quotes(currency),
            amount: strip_quotes(amount),
            transaction_type: strip_quotes(transaction_type),
            payee: strip_quotes(payee),
        }
    }
    pub fn header() -> StringRecord {
        let mut record = StringRecord::new();
        record.push_field("Date");
        record.push_field("Amount");
        record.push_field("Type");
        record.push_field("Payee");
        record
    }

    pub fn to_record(&self) -> StringRecord {
        let mut record = StringRecord::new();
        record.push_field(&self.date.format("%Y-%m-%d").to_string());
        record.push_field(&self.amount);
        record.push_field(&self.transaction_type);
        record.push_field(&self.payee);
        record
    }
}

pub fn header_contains_string(header: &StringRecord, pattern: &str) -> bool {
    for field in header.iter() {
        if field.contains(pattern) {
            return true;
        }
    }
    false
}
