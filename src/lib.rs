//! Detect CSV files from a couple of German banks (N26, DKB) and PayPal,
//! filter out transactions in a specific currency and generate a CSV file with these transactions
use chrono::NaiveDate;
use csv::StringRecord;
use encoding_rs::ISO_8859_10;
use std::cmp::Ordering;
use std::collections::HashMap;
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
/// The number of first columns to read from the CSV file; used to detect the source
pub const NUM_FIRST_COLUMNS: usize = 5;
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
const N26_COLUMNS_2024_09: [&str; NUM_FIRST_COLUMNS] = [
    "Booking Date",
    "Value Date",
    "Partner Name",
    "Partner Iban",
    "Type",
];
const DKB_COLUMNS: [&str; NUM_FIRST_COLUMNS] = [
    "Buchungstag",
    "Wertstellung",
    "Buchungstext",
    "Auftraggeber / Begünstigter",
    "Verwendungszweck",
];
const DKB_COLUMNS_2024_09: [&str; NUM_FIRST_COLUMNS] = [
    "Buchungsdatum",
    "Wertstellung",
    "Status",
    "Zahlungspflichtige*r",
    "Zahlungsempfänger*in",
];

/// The source of a CSV file
#[derive(Debug, PartialEq)]
pub enum Source {
    /// N26 CSV
    N26,
    /// PayPal has changed the CSV format at least once
    PayPal,
    /// DKB has a weird CSV with some lines on the top that don't match the rest of the file
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

/// Detect the separator of a CSV file
///
/// # Arguments
///
/// * `file_path`: Path to the CSV file
///
/// returns: Result<(u8, Option<Source>), Error>
pub fn detect_separator(file_path: &Path) -> io::Result<(u8, Option<Source>)> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    if let Some(line) = reader.lines().next() {
        let first_line = line?;

        // DKB has a weird CSV with some lines on the top that don't match the rest of the file
        let source = if first_line.contains("Girokonto") {
            Some(Source::DKB)
        } else {
            None
        };

        if first_line.contains(';') {
            Ok((b';', source))
        } else if first_line.contains(',') {
            Ok((b',', source))
        } else if first_line.contains('\t') {
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

/// Remove the first extra lines from a DKB CSV file
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
        if line_content.contains("Verwendungszweck") {
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

/// Filter rows from a CSV file by currency and determine the source based on the first columns
///
/// # Arguments
///
/// * `file_path`: path to the CSV file to read
/// * `separator`: field delimiter byte (e.g. b',' or b';')
/// * `upper_currency`: the currency to filter by, in uppercase (EUR, USD, ...)
///
/// returns: `Result<(Source, Vec<CsvOutputRow>), Error>`
pub fn filter_data_frame(
    file_path: &Path,
    separator: u8,
    upper_currency: &str,
) -> Result<(Source, Vec<CsvOutputRow>), Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(separator)
        .has_headers(true)
        .flexible(true)
        .from_reader(file);

    // Read header and build column map
    let header_record = rdr.headers()?.clone();
    let headers: Vec<String> = header_record.iter().map(|s| s.to_string()).collect();
    let first_columns: Vec<&str> = headers
        .iter()
        .take(NUM_FIRST_COLUMNS)
        .map(|s| s.as_str())
        .collect();

    // Build column index map
    let col_map: HashMap<&str, usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| (h.as_str(), i))
        .collect();

    let source: Source;
    let mut rows: Vec<CsvOutputRow> = Vec::new();

    // TODO: move these configs to separate structs or enums instead of "if" statements
    if first_columns == PAYPAL_COLUMNS {
        source = Source::PayPal;
        for result in rdr.records() {
            let record = result?;
            let currency = get_field(&record, &col_map, "Currency");
            let balance_impact = get_field(&record, &col_map, "Balance Impact");
            let transaction_type = get_field(&record, &col_map, "Type");
            // Filter: currency must match, must be Debit, not General Currency Conversion
            if currency != upper_currency {
                continue;
            }
            if balance_impact != "Debit" {
                continue;
            }
            if transaction_type == "General Currency Conversion" {
                continue;
            }
            let date_str = get_field(&record, &col_map, "Date");
            let amount = get_field(&record, &col_map, "Gross");
            let payee = get_field(&record, &col_map, "Name");
            let bank_id = get_field(&record, &col_map, "Transaction ID");
            let date = parse_date(&date_str)?;
            rows.push(CsvOutputRow::new(
                date,
                Source::PayPal.to_string(),
                currency,
                amount,
                transaction_type,
                payee,
                String::new(),
                bank_id,
            ));
        }
    } else if first_columns == PAYPAL_COLUMNS_OLD {
        source = Source::PayPal;
        for result in rdr.records() {
            let record = result?;
            let currency = get_field(&record, &col_map, "Currency");
            let description = get_field(&record, &col_map, "Description");
            // Filter: currency must match, not General Currency Conversion
            if currency != upper_currency {
                continue;
            }
            if description == "General Currency Conversion" {
                continue;
            }
            let date_str = get_field(&record, &col_map, "Date");
            let amount = get_field(&record, &col_map, "Gross");
            let payee = get_field(&record, &col_map, "Name");
            let bank_id = get_field(&record, &col_map, "Transaction ID");
            let date = parse_date(&date_str)?;
            rows.push(CsvOutputRow::new(
                date,
                Source::PayPal.to_string(),
                currency,
                amount,
                description,
                payee,
                String::new(),
                bank_id,
            ));
        }
    } else if first_columns == N26_COLUMNS {
        source = Source::N26;
        for result in rdr.records() {
            let record = result?;
            let currency_val = get_field(&record, &col_map, "Type Foreign Currency");
            // For EUR: include rows with empty currency or "EUR" (N26 is not consistent)
            if upper_currency == "EUR" {
                if !currency_val.is_empty() && currency_val != "EUR" {
                    continue;
                }
            } else if currency_val != upper_currency {
                continue;
            }
            let date_str = get_field(&record, &col_map, "Date");
            let amount = get_field(&record, &col_map, "Amount (EUR)");
            let transaction_type = get_field(&record, &col_map, "Transaction type");
            let payee = get_field(&record, &col_map, "Payee");
            let memo = get_field(&record, &col_map, "Payment reference");
            let date = parse_date(&date_str)?;
            rows.push(CsvOutputRow::new(
                date,
                Source::N26.to_string(),
                if currency_val.is_empty() {
                    "EUR".to_string()
                } else {
                    currency_val
                },
                amount,
                transaction_type,
                payee,
                memo,
                String::new(),
            ));
        }
    } else if first_columns == N26_COLUMNS_2024_09 {
        source = Source::N26;
        for result in rdr.records() {
            let record = result?;
            let currency_val = get_field(&record, &col_map, "Original Currency");
            // For EUR: include rows with empty currency or "EUR" (N26 is not consistent)
            if upper_currency == "EUR" {
                if !currency_val.is_empty() && currency_val != "EUR" {
                    continue;
                }
            } else if currency_val != upper_currency {
                continue;
            }
            let date_str = get_field(&record, &col_map, "Booking Date");
            let amount_raw = get_field(&record, &col_map, "Amount (EUR)");
            let transaction_type = get_field(&record, &col_map, "Type");
            let payee = get_field(&record, &col_map, "Partner Name");
            let memo = get_field(&record, &col_map, "Payment Reference");
            let date = parse_date(&date_str)?;
            // N26 new format: "Presentment" transactions have positive amounts that represent debits
            let amount = if transaction_type == "Presentment" {
                format!("-{}", amount_raw.trim_start_matches('-'))
            } else {
                amount_raw
            };
            rows.push(CsvOutputRow::new(
                date,
                Source::N26.to_string(),
                if currency_val.is_empty() {
                    "EUR".to_string()
                } else {
                    currency_val
                },
                amount,
                transaction_type,
                payee,
                memo,
                String::new(),
            ));
        }
    } else if first_columns == DKB_COLUMNS {
        source = Source::DKB;
        for result in rdr.records() {
            let record = result?;
            let memo = get_field(&record, &col_map, "Verwendungszweck");
            let mut currency = if upper_currency == "EUR" {
                "EUR".to_string()
            } else {
                upper_currency.to_string()
            };
            let mut amount = get_field(&record, &col_map, "Betrag (EUR)");
            if upper_currency != "EUR" {
                match dkb_extract_amount(&currency, &memo) {
                    None => continue,
                    Some(extracted) => {
                        amount = if amount.contains('-') {
                            format!("-{}", extracted)
                        } else {
                            extracted
                        };
                    }
                }
            }
            let date_str = get_field(&record, &col_map, "Buchungstag");
            let transaction_type = get_field(&record, &col_map, "Buchungstext");
            let payee = get_field(&record, &col_map, "Auftraggeber / Begünstigter");
            let bank_id = col_map
                .get("Kundenreferenz")
                .and_then(|&i| record.get(i))
                .unwrap_or("")
                .to_string();
            let date = parse_date(&date_str)?;
            // Normalise DKB amount: German thousands/decimal -> dot-decimal
            let amount_dot = normalize_german_amount(&amount);
            currency = currency.replace(CHAR_COMMA, CHAR_DOT);
            let (orig_amount, orig_currency) = dkb_extract_fx(&memo).unzip();
            let mut row = CsvOutputRow::new(
                date,
                Source::DKB.to_string(),
                currency,
                amount_dot,
                transaction_type,
                payee,
                memo,
                bank_id,
            );
            row.original_amount = orig_amount.unwrap_or_default();
            row.original_currency = orig_currency.unwrap_or_default();
            rows.push(row);
        }
        return Ok((source, rows));
    } else if first_columns == DKB_COLUMNS_2024_09 {
        source = Source::DKB;
        for result in rdr.records() {
            let record = result?;
            let memo = get_field(&record, &col_map, "Verwendungszweck");
            let mut currency = if upper_currency == "EUR" {
                "EUR".to_string()
            } else {
                upper_currency.to_string()
            };
            let mut amount = get_field(&record, &col_map, "Betrag (€)");
            if upper_currency != "EUR" {
                match dkb_extract_amount(&currency, &memo) {
                    None => continue,
                    Some(extracted) => {
                        amount = if amount.contains('-') {
                            format!("-{}", extracted)
                        } else {
                            extracted
                        };
                    }
                }
            }
            let date_str = get_field(&record, &col_map, "Buchungsdatum");
            let transaction_type = get_field(&record, &col_map, "Umsatztyp");
            let payee = get_field(&record, &col_map, "Zahlungsempfänger*in");
            let bank_id = col_map
                .get("Kundenreferenz")
                .and_then(|&i| record.get(i))
                .unwrap_or("")
                .to_string();
            let date = parse_date(&date_str)?;
            // Normalise DKB amount: German thousands/decimal -> dot-decimal
            let amount_dot = normalize_german_amount(&amount);
            currency = currency.replace(CHAR_COMMA, CHAR_DOT);
            let (orig_amount, orig_currency) = dkb_extract_fx(&memo).unzip();
            let mut row = CsvOutputRow::new(
                date,
                Source::DKB.to_string(),
                currency,
                amount_dot,
                transaction_type,
                payee,
                memo,
                bank_id,
            );
            row.original_amount = orig_amount.unwrap_or_default();
            row.original_currency = orig_currency.unwrap_or_default();
            rows.push(row);
        }
        return Ok((source, rows));
    } else {
        return Err(format!(
            "{}: unknown CSV format (first columns: {:?})",
            file_path.display(),
            first_columns
        )
        .into());
    }

    Ok((source, rows))
}

/// Get a field value from a record by column name, returning empty string if not found
fn get_field(record: &StringRecord, col_map: &HashMap<&str, usize>, col_name: &str) -> String {
    col_map
        .get(col_name)
        .and_then(|&i| record.get(i))
        .unwrap_or("")
        .to_string()
}

/// Parse a date string in multiple formats (ISO, German with 2-digit year, German with 4-digit year)
fn parse_date(date_str: &str) -> Result<NaiveDate, Box<dyn std::error::Error>> {
    // Try ISO 8601 (N26, PayPal: YYYY-MM-DD)
    if let Ok(d) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        return Ok(d);
    }
    // Try DD/MM/YYYY (PayPal DE locale and others)
    if let Ok(d) = NaiveDate::parse_from_str(date_str, "%d/%m/%Y") {
        return Ok(d);
    }
    // Try MM/DD/YYYY (PayPal US locale)
    if let Ok(d) = NaiveDate::parse_from_str(date_str, "%m/%d/%Y") {
        return Ok(d);
    }
    // Try DKB new 2-digit year (DD.MM.YY)
    if let Ok(d) = NaiveDate::parse_from_str(date_str, "%d.%m.%y") {
        return Ok(d);
    }
    // Try DKB old 4-digit year (DD.MM.YYYY)
    if let Ok(d) = NaiveDate::parse_from_str(date_str, "%d.%m.%Y") {
        return Ok(d);
    }
    Err(format!("Cannot parse date: {}", date_str).into())
}

/// Extract the original foreign-currency amount and currency code from a DKB memo.
///
/// Returns `Some((dot_decimal_amount, currency_code))` when the memo contains an FX marker
/// (`" 1 Euro="`), or `None` when no FX data is present.
///
/// # Examples
///
/// ```
/// use bank_csv::dkb_extract_fx;
/// assert_eq!(
///     dkb_extract_fx("2023-12-13      Debitk.44 Original 12,00 BRL 1 Euro=5,28634270 BRL VISA Debit"),
///     Some(("12.00".to_string(), "BRL".to_string()))
/// );
/// assert_eq!(dkb_extract_fx("Normal domestic payment"), None);
/// ```
pub fn dkb_extract_fx(memo: &str) -> Option<(String, String)> {
    if !memo.contains(" 1 Euro=") {
        return None;
    }
    // Extract the currency code: the word immediately before " 1 Euro="
    let euro_pos = memo.find(" 1 Euro=")?;
    let before = memo[..euro_pos].trim_end();
    let currency = before.split_whitespace().next_back()?.to_string();
    if currency.len() < 2 || currency.len() > 4 || !currency.chars().all(|c| c.is_ascii_uppercase())
    {
        return None;
    }
    let raw_amount = dkb_extract_amount(&currency, memo)?;
    Some((normalize_german_amount(&raw_amount), currency))
}

/// Extract the amount from a DKB memo
///
/// # Arguments
///
/// * `currency`: 3-letter currency code
/// * `memo`: The memo or description of the transaction
///
/// returns: `Option<String>`
///
/// # Examples
///
/// ```
/// use bank_csv::dkb_extract_amount;
/// assert_eq!(dkb_extract_amount("BRL", "2023-12-12      Debitk.44 Original 6,99 BRL 1 Euro=5,29545460 BRL VISA Debit"), Some("6,99".to_string()));
/// assert_eq!(dkb_extract_amount("BRL", "Nothing here"), None);
/// assert_eq!(dkb_extract_amount("BRL", "VISA Debitkartenumsatz in Fremdwährung / Ursprungsbetrag in Fremdwährung 19,90 BRL / Umrechnungsrate: 1 Euro=6,03030470 BRL"), Some("19,90".to_string()));
pub fn dkb_extract_amount(currency: &str, memo: &str) -> Option<String> {
    if !memo.contains(" 1 Euro=") {
        return None;
    }

    let original_keyword = "Original ";
    let fremdwaehrung = "Ursprungsbetrag in Fremdwährung ";
    let start;
    let word_length;
    if memo.contains(original_keyword) {
        start = memo.find(original_keyword)?;
        word_length = original_keyword.len();
    } else if memo.contains(fremdwaehrung) {
        start = memo.find(fremdwaehrung)?;
        word_length = fremdwaehrung.len();
    } else {
        eprintln!("Could not extract amount from DKB memo: {}", memo);
        return None;
    }

    let end = memo.find(currency)?;
    if end <= start {
        return None;
    }

    let amount_start = start + word_length;
    let amount = &memo[amount_start..end].trim();

    Some(amount.to_string())
}

/// Normalise a German-format amount string to dot-decimal.
///
/// German numbers use `.` as the thousands separator and `,` as the decimal
/// separator.  Two-step conversion:
/// 1. Remove thousands-separator dots (a dot followed by exactly 3 digits,
///    then end-of-string, another dot, or a comma).
/// 2. Replace the remaining decimal comma with a dot.
///
/// # Examples
///
/// ```
/// use bank_csv::normalize_german_amount;
/// assert_eq!(normalize_german_amount("-5.678"),    "-5678");
/// assert_eq!(normalize_german_amount("-1.234,56"), "-1234.56");
/// assert_eq!(normalize_german_amount("-34,14"),    "-34.14");
/// assert_eq!(normalize_german_amount("6.089,31"),  "6089.31");
/// assert_eq!(normalize_german_amount("-0,81"),     "-0.81");
/// assert_eq!(normalize_german_amount("300"),       "300");
/// assert_eq!(normalize_german_amount("-665"),      "-665");
/// ```
pub fn normalize_german_amount(amount: &str) -> String {
    let bytes = amount.as_bytes();
    let mut result = String::with_capacity(amount.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'.' {
            // Check whether this dot is a thousands separator:
            // it must be followed by exactly 3 ASCII digits, then either
            // end-of-string, another dot, or a comma.
            let after = i + 1;
            let is_thousands = after + 3 <= bytes.len()
                && bytes[after].is_ascii_digit()
                && bytes[after + 1].is_ascii_digit()
                && bytes[after + 2].is_ascii_digit()
                && (after + 3 == bytes.len()
                    || bytes[after + 3] == b'.'
                    || bytes[after + 3] == b',');
            if is_thousands {
                // Skip the thousands-separator dot; keep the 3 digits.
                i += 1;
                continue;
            }
        }
        // Replace decimal comma with dot.
        result.push(if bytes[i] == b',' {
            '.'
        } else {
            bytes[i] as char
        });
        i += 1;
    }
    result
}

/// A row in the CSV output
#[derive(PartialEq, Eq)]
pub struct CsvOutputRow {
    /// The date of the transaction
    pub date: NaiveDate,
    /// The source of the transaction (PayPal, N26, DKB)
    pub source: String,
    /// The currency of the transaction, 3 letters (EUR, USD, ...)
    pub currency: String,
    /// The amount of the transaction, stored as dot-decimal
    pub amount: String,
    /// The type of the transaction, read from the original CSV
    pub transaction_type: String,
    /// The payee of the transaction
    pub payee: String,
    /// The memo or description of the transaction
    pub memo: String,
    /// The bank-native transaction ID when available
    pub bank_id: String,
    /// Original amount in foreign currency (dot-decimal); empty string when no FX
    pub original_amount: String,
    /// Original currency code (e.g. "BRL"); empty string when no FX
    pub original_currency: String,
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
        let direction = if self.amount.contains('-') {
            "paid to"
        } else {
            "from"
        };
        write!(
            f,
            "{} [{}] {} {} {} {} ({})",
            self.date,
            self.source,
            self.currency,
            self.amount,
            direction,
            self.payee,
            self.transaction_type,
        )
    }
}

/// Strip double quotes from the content of a field. Needed for the N26 CSV file.
pub fn strip_quotes(s: String) -> String {
    s.strip_prefix(CHAR_DOUBLE_QUOTE)
        .unwrap_or(s.as_str())
        .strip_suffix(CHAR_DOUBLE_QUOTE)
        .unwrap_or(s.as_str())
        .to_string()
}

impl CsvOutputRow {
    /// Create a new CsvOutputRow
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        date: NaiveDate,
        source: String,
        currency: String,
        amount: String,
        transaction_type: String,
        payee: String,
        memo: String,
        bank_id: String,
    ) -> Self {
        // Assume euros if the currency is empty or "null" (thanks DKB and N26)
        let stripped = strip_quotes(currency);
        let final_currency = if stripped.is_empty() || stripped == "null" {
            "EUR"
        } else {
            stripped.as_str()
        };

        Self {
            date,
            source,
            currency: final_currency.to_string(),
            // Store amount as dot-decimal (raw); comma conversion is merge command's responsibility
            amount: strip_quotes(amount),
            transaction_type: strip_quotes(transaction_type),
            payee: strip_quotes(payee),
            memo: strip_quotes(memo),
            bank_id: strip_quotes(bank_id),
            original_amount: String::new(),
            original_currency: String::new(),
        }
    }

    /// Create a CSV header
    pub fn header() -> StringRecord {
        let mut record = StringRecord::new();
        record.push_field("Date");
        record.push_field("Source");
        record.push_field("Currency");
        record.push_field("Amount");
        record.push_field("Type");
        record.push_field("Payee");
        record.push_field("Memo");
        record.push_field("BankId");
        record.push_field("OriginalAmount");
        record.push_field("OriginalCurrency");
        record
    }

    /// Convert a CsvOutputRow to a passthrough CSV record (dot-decimal amounts, ISO 8601 dates)
    pub fn to_passthrough_record(&self) -> StringRecord {
        let mut record = StringRecord::new();
        record.push_field(&self.date.format("%Y-%m-%d").to_string());
        record.push_field(&self.source);
        record.push_field(&self.currency);
        record.push_field(&self.amount);
        record.push_field(&self.transaction_type);
        record.push_field(&self.payee);
        record.push_field(&self.memo);
        record.push_field(&self.bank_id);
        record.push_field(&self.original_amount);
        record.push_field(&self.original_currency);
        record
    }

    /// Convert a CsvOutputRow to a CSV record (merge format: comma-decimal amounts)
    pub fn to_record(&self) -> StringRecord {
        let mut record = StringRecord::new();
        record.push_field(&self.date.format("%Y-%m-%d").to_string());
        record.push_field(&self.source);
        record.push_field(&self.currency);
        // Merge format uses comma-decimal for macOS Numbers compatibility
        record.push_field(&self.amount.replace(CHAR_DOT, CHAR_COMMA));
        record.push_field(&self.transaction_type);
        record.push_field(&self.payee);
        record.push_field(&self.memo);
        record.push_field(&self.bank_id);
        record.push_field(&self.original_amount);
        record.push_field(&self.original_currency);
        record
    }
}
