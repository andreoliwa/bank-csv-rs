use chrono::NaiveDate;
use csv::StringRecord;
use encoding_rs::ISO_8859_10;
use polars::prelude::*;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Display;
use std::fs::File;
use std::io::Read;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use tempfile::NamedTempFile;

const CHAR_COMMA: &str = ",";
const CHAR_DOT: &str = ".";
const CHAR_DOUBLE_QUOTE: char = '"';
pub const NUM_FIRST_COLUMNS: usize = 5;
pub const NUM_SELECT_COLUMNS: usize = 6;
const PAYPAL_COLUMNS: [&str; NUM_FIRST_COLUMNS] = ["Date", "Time", "TimeZone", "Name", "Type"];
const PAYPAL_COLUMNS_OLD: [&str; NUM_FIRST_COLUMNS] =
    ["Date", "Time", "Time Zone", "Description", "Currency"];
const N26_COLUMNS: [&str; NUM_FIRST_COLUMNS] = [
    "Date",
    "Payee",
    "Account number",
    "Transaction type",
    "Payment reference",
];
const DKB_COLUMNS: [&str; NUM_FIRST_COLUMNS] = [
    "Buchungstag",
    "Wertstellung",
    "Buchungstext",
    "Auftraggeber / Begünstigter",
    "Verwendungszweck",
];

#[derive(PartialEq)]
pub enum Source {
    N26,
    PayPal,
    DKB,
}

impl Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Source::N26 => "N26".to_string(),
            Source::PayPal => "PayPal".to_string(),
            Source::DKB => "DKB".to_string(),
        };
        write!(f, "{}", str)
    }
}

pub fn detect_separator(file_path: &Path) -> io::Result<(u8, Option<Source>)> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    if let Some(first_line) = reader.lines().next() {
        let line = first_line?;

        // DKB has a weird CSV with some lines on the top that don't match the rest of the file
        let source = if line.contains("Kontonummer:") {
            Some(Source::DKB)
        } else {
            None
        };

        if line.contains(';') {
            Ok((b';', source))
        } else if line.contains(',') {
            Ok((b',', source))
        } else if line.contains('\t') {
            Ok((b'\t', source))
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

/// Remove the first extra lines from a DKB CSV file.
///
/// # Arguments
///
/// * `original_dkb_csv_file`: path to the original DKB CSV file
/// * `temp_file`:  a temporary file to write the filtered CSV to
///
/// returns: Result<(), Error>
pub fn dkb_edit_file(
    original_dkb_csv_file: &Path,
    mut temp_file: &NamedTempFile,
) -> io::Result<()> {
    let input_file = File::open(original_dkb_csv_file)?;
    let input_reader = BufReader::new(input_file);
    let mut temp_writer = BufWriter::new(&mut temp_file);

    let mut buffer = Vec::new();
    input_reader.take(u64::MAX).read_to_end(&mut buffer)?;
    let (decoded, _, _) = ISO_8859_10.decode(&buffer);
    let mut write_lines = false;
    for line_content in decoded.lines() {
        if line_content.contains("Buchungstag") {
            write_lines = true;
        }
        if write_lines {
            writeln!(temp_writer, "{}", line_content)?;
        }
    }

    // Flush the writer to make sure everything is written to the temporary file
    temp_writer.flush()?;

    Ok(())
}

pub fn filter_data_frame(df: &DataFrame, upper_currency: String) -> (Source, DataFrame) {
    let schema = df.schema();
    let first_columns: Vec<&str> = schema
        .iter_names()
        .take(NUM_FIRST_COLUMNS)
        .map(|field| field.as_str())
        .collect();

    let columns_to_select: [&str; NUM_SELECT_COLUMNS];
    let source: Source;
    let lazy_frame: LazyFrame;
    let cloned_df = df.clone();

    // TODO: move these configs to separate structs or enums instead of "if" statements
    if first_columns == PAYPAL_COLUMNS {
        source = Source::PayPal;
        columns_to_select = [
            "Date",
            "Currency",
            "Gross",
            "Type",
            "Name",
            "Transaction ID",
        ];
        lazy_frame = cloned_df
            .lazy()
            .filter(col("Currency").eq(lit(upper_currency.as_str())))
            .filter(col("Balance Impact").eq(lit("Debit")))
            .filter(col("Type").neq(lit("General Currency Conversion")));
    } else if first_columns == PAYPAL_COLUMNS_OLD {
        source = Source::PayPal;
        columns_to_select = [
            "Date",
            "Currency",
            "Gross",
            "Description",
            "Name",
            "Transaction ID",
        ];
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
            "Payment reference",
        ];
        lazy_frame = cloned_df
            .lazy()
            .filter(col("Type Foreign Currency").eq(lit(upper_currency.as_str())))
    } else if first_columns == DKB_COLUMNS {
        source = Source::DKB;
        columns_to_select = [
            "Buchungstag",
            // Use any non-duplicated column here, otherwise polars will panic with:
            // "column with name 'Verwendungszweck' has more than one occurrences".
            // The memo (Verwendungszweck = "intended use") contains the foreign currency.
            // We will filter and replace the value of this column later.
            "Mandatsreferenz",
            "Betrag (EUR)",
            "Buchungstext",
            "Auftraggeber / Begünstigter",
            "Verwendungszweck",
        ];
        // Filtering will be done manually because DKB doesn't have a currency column
        lazy_frame = cloned_df.lazy()
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

pub fn dkb_extract_amount(currency: &str, memo: &str) -> Option<String> {
    let original_keyword = "Original ";
    let start = memo.find(original_keyword)?;
    let end = memo.find(currency)?;
    if end <= start {
        return None;
    }

    let amount_start = start + original_keyword.len();
    let amount = &memo[amount_start..end].trim();

    Some(amount.to_string())
}

#[derive(PartialEq, Eq)]
pub struct CsvOutputRow {
    pub date: NaiveDate,
    pub source: String,
    pub currency: String,
    pub amount: String,
    pub transaction_type: String,
    pub payee: String,
    pub memo: String,
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
    s.strip_prefix(CHAR_DOUBLE_QUOTE)
        .unwrap_or(s.as_str())
        .strip_suffix(CHAR_DOUBLE_QUOTE)
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
        memo: String,
    ) -> Self {
        Self {
            date,
            source,
            currency: strip_quotes(currency),
            // "Numbers" on my macOS only understands commas as decimal separators;
            // I can make it configurable if someone ever uses this crate
            amount: strip_quotes(amount).replace(CHAR_DOT, CHAR_COMMA),
            transaction_type: strip_quotes(transaction_type),
            payee: strip_quotes(payee),
            memo: strip_quotes(memo),
        }
    }
    pub fn header() -> StringRecord {
        let mut record = StringRecord::new();
        record.push_field("Date");
        record.push_field("Source");
        record.push_field("Currency");
        record.push_field("Amount");
        record.push_field("Type");
        record.push_field("Payee");
        record.push_field("Memo");
        record
    }

    pub fn to_record(&self) -> StringRecord {
        let mut record = StringRecord::new();
        record.push_field(&self.date.format("%Y-%m-%d").to_string());
        record.push_field(&self.source);
        record.push_field(&self.currency);
        record.push_field(&self.amount);
        record.push_field(&self.transaction_type);
        record.push_field(&self.payee);
        record.push_field(&self.memo);
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
