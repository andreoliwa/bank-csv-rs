use chrono::NaiveDate;
use csv::StringRecord;
use polars::prelude::*;
use serde::Deserialize;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Display;

pub enum CsvSource {
    N26,
    PayPal,
}

impl Display for CsvSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            CsvSource::N26 => "N26".to_string(),
            CsvSource::PayPal => "PayPal".to_string(),
        };
        write!(f, "{}", str)
    }
}

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

pub fn filter_data_frame(df: &DataFrame, upper_currency: String) -> (CsvSource, DataFrame) {
    let schema = df.schema();
    let first_columns: Vec<&str> = schema
        .iter_names()
        .take(NUM_COLUMNS)
        .map(|field| field.as_str())
        .collect();

    let columns_to_select: [&str; NUM_COLUMNS];
    let source: CsvSource;
    let lazy_frame: LazyFrame;
    let cloned_df = df.clone();

    if first_columns == PAYPAL_COLUMNS {
        source = CsvSource::PayPal;
        columns_to_select = ["Date", "Currency", "Gross", "Type", "Name"];
        lazy_frame = cloned_df
            .lazy()
            .filter(col("Currency").eq(lit(upper_currency.as_str())))
            .filter(col("Balance Impact").eq(lit("Debit")));
    } else if first_columns == PAYPAL_COLUMNS_OLD {
        source = CsvSource::PayPal;
        columns_to_select = ["Date", "Currency", "Gross", "Description", "Name"];
        lazy_frame = cloned_df
            .lazy()
            .filter(col("Currency").eq(lit(upper_currency.as_str())))
            .filter(col("Description").neq(lit("General Currency Conversion")));
    } else if first_columns == N26_COLUMNS {
        source = CsvSource::N26;
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

pub trait BaseTransaction {
    fn identification_column() -> &'static str;
    fn valid(&self, currency: &str) -> bool;
    fn to_csv_transaction(&self) -> CsvTransaction;
}

#[derive(Debug, Deserialize)]
pub struct N26Transaction {
    #[serde(rename = "Date")]
    date_str: String,
    #[serde(rename = "Payee")]
    payee: String,
    #[serde(rename = "Account number")]
    account_number: String,
    #[serde(rename = "Transaction type")]
    transaction_type: String,
    #[serde(rename = "Payment reference")]
    payment_reference: String,
    #[serde(rename = "Amount (EUR)")]
    eur_amount_str: String,
    #[serde(rename = "Amount (Foreign Currency)")]
    foreign_amount_str: String,
    #[serde(rename = "Type Foreign Currency")]
    currency: String,
    #[serde(rename = "Exchange Rate")]
    exchange_rate_str: String,
}

impl fmt::Display for N26Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Date={} Payee={} Account={} Type={} Reference={} EUR={} {}={} Exchange={}",
            self.date_str,
            self.payee,
            self.account_number,
            self.transaction_type,
            self.payment_reference,
            self.eur_amount(),
            self.currency,
            self.foreign_amount(),
            self.exchange_rate_str
        )
    }
}

impl BaseTransaction for N26Transaction {
    fn identification_column() -> &'static str {
        "Amount (Foreign Currency)"
    }

    fn valid(&self, currency: &str) -> bool {
        self.currency.to_uppercase() == currency.to_uppercase()
    }

    fn to_csv_transaction(&self) -> CsvTransaction {
        let amount = if self.currency.to_uppercase() == "EUR" {
            self.eur_amount_str.clone()
        } else {
            self.foreign_amount_str.clone()
        };
        CsvTransaction {
            date: self.date(),
            source: "N26".to_string(),
            currency: self.currency.clone(),
            amount,
            transaction_type: self.transaction_type.clone(),
            payee: self.payee.clone(),
        }
    }
}

impl N26Transaction {
    fn date(&self) -> NaiveDate {
        NaiveDate::parse_from_str(&self.date_str, "%Y-%m-%d").expect("Invalid date string")
    }

    fn eur_amount(&self) -> f64 {
        self.eur_amount_str.parse().expect("Invalid amount format")
    }

    fn foreign_amount(&self) -> f64 {
        self.foreign_amount_str
            .parse()
            .expect("Invalid amount format")
    }
}

#[derive(Debug, Deserialize)]
pub struct PayPalTransaction {
    #[serde(rename = "Date")]
    date_str: String,
    #[serde(rename = "Time")]
    time_str: String,
    #[serde(rename = "Time Zone")]
    time_zone: String,
    // TODO: PayPal has changed the CSV columns... validate and accept multiple CSV formats
    // #[serde(rename = "Description")]
    // description: String,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Type")]
    type_: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Currency")]
    currency: String,
    #[serde(rename = "Gross")]
    gross_str: String,
    #[serde(rename = "Fee")]
    fee_str: String,
    #[serde(rename = "Net")]
    net_str: String,
    #[serde(rename = "Balance")]
    balance_str: String,
    #[serde(rename = "Transaction ID")]
    transaction_id: String,
    #[serde(rename = "From Email Address")]
    from_email_address: String,
    #[serde(rename = "Bank Name")]
    bank_name: String,
    #[serde(rename = "Bank Account")]
    bank_account: String,
    #[serde(rename = "Shipping and Handling Amount")]
    shipping_and_handling_amount_str: String,
    #[serde(rename = "Sales Tax")]
    sales_tax_str: String,
    #[serde(rename = "Invoice ID")]
    invoice_id: String,
    #[serde(rename = "Reference Txn ID")]
    reference_txn_id: String,
}

impl fmt::Display for PayPalTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Date={} Time={} Timezone={} Currency={} Gross={} Fee={} Net={} Balance={} TransactionID={}\
             From={} Name={} BankName={} BankAccount={} Shipping={} Tax={} Invoice={} Reference={} Status={}",
            self.date_str,
            self.time_str,
            self.time_zone,
            self.currency,
            self.gross_str,
            self.fee_str,
            self.net_str,
            self.balance_str,
            self.transaction_id,
            self.from_email_address,
            self.name,
            self.bank_name,
            self.bank_account,
            self.shipping_and_handling_amount_str,
            self.sales_tax_str,
            self.invoice_id,
            self.reference_txn_id,
            self.status,
        )
    }
}

impl BaseTransaction for PayPalTransaction {
    fn identification_column() -> &'static str {
        "Shipping and Handling Amount"
    }

    // TODO: this method is probably not necessary anymore, now that we have data frame filters
    fn valid(&self, currency: &str) -> bool {
        self.currency.to_uppercase() == currency.to_uppercase() && !self.name.is_empty()
    }

    fn to_csv_transaction(&self) -> CsvTransaction {
        CsvTransaction {
            date: self.date(),
            source: "PayPal".to_string(),
            currency: self.currency.clone(),
            amount: self.gross_str.clone(),
            transaction_type: self.type_.clone(),
            payee: self.name.clone(),
        }
    }
}

impl PayPalTransaction {
    fn date(&self) -> NaiveDate {
        NaiveDate::parse_from_str(&self.date_str, "%d.%m.%Y").expect("Invalid date string")
    }
}

#[derive(PartialEq, Eq)]
pub struct CsvTransaction {
    pub date: NaiveDate,
    pub source: String,
    pub currency: String,
    pub amount: String,
    pub transaction_type: String,
    pub payee: String,
}

impl PartialOrd for CsvTransaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CsvTransaction {
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

impl fmt::Display for CsvTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Date={} {} {} {} to {} ({})",
            self.date, self.source, self.currency, self.amount, self.payee, self.transaction_type,
        )
    }
}

const DOUBLE_QUOTE: char = '"';
fn strip_quotes(s: String) -> String {
    s.strip_prefix(DOUBLE_QUOTE)
        .unwrap_or(s.as_str())
        .strip_suffix(DOUBLE_QUOTE)
        .unwrap_or(s.as_str())
        .to_string()
}

impl CsvTransaction {
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

// TODO: try harder; this fails with:
//  ❯ cargo fmt && cargo build
//     Compiling money v0.1.0 (/Users/aa/Code/money)
//  error[E0277]: the trait bound `T: _::_serde::Deserialize<'_>` is not satisfied
//     --> src/main.rs:389:51
//      |
//  389 |         let transaction: T = record.deserialize::<T>(None)?;
//      |                                                   ^ the trait `_::_serde::Deserialize<'_>` is not implemented for `T`
//      |
//  note: required by a bound in `StringRecord::deserialize`
//     --> /Users/aa/.cargo/registry/src/github.com-1ecc6299db9ec823/csv-1.2.1/src/string_record.rs:292:32
//      |
//  292 |     pub fn deserialize<'de, D: Deserialize<'de>>(
//      |                                ^^^^^^^^^^^^^^^^ required by this bound in `StringRecord::deserialize`
//  help: consider further restricting this bound
//      |
//  376 | fn parse_csv_as<T: BaseTransaction + _::_serde::Deserialize<'_>>(
//      |                                    ++++++++++++++++++++++++++++
//  For more information about this error, try `rustc --explain E0277`.
//  error: could not compile `money` due to previous error

// fn parse_csv_as<T: BaseTransaction>(
//     currency: &String,
//     currency_transactions: &mut SortedSet<CsvTransaction>,
//     rdr: &mut Reader<File>,
//     header: &StringRecord,
// ) -> Result<bool, Box<dyn Error>> {
//     if !header_contains_string(&header, T::identification_column()) {
//         return Ok(false);
//     }
//
//     for result in rdr.records() {
//         let record = result?;
//
//         let transaction: T = record.deserialize::<T>(None)?;
//         if transaction.valid(&currency) {
//             currency_transactions.push(transaction.to_csv_transaction());
//         }
//     }
//     Ok(true)
// }
