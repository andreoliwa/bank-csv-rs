use bank_csv::CsvTransaction;
use chrono::{Datelike, NaiveDate};
use clap::{Parser, Subcommand};
use csv::Writer;
use polars::export::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use polars::frame::row::Row;
use polars::prelude::*;
use sorted_vec::SortedSet;
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Merge one or more bank CSV files and split them into multiple files, one for each month
    #[command(arg_required_else_help = true)]
    Merge {
        /// Path(s) to the CSV file(s) to be parsed
        csv_file_paths: Vec<PathBuf>,
        /// Currency to filter (case insensitive)
        #[arg(short, long, default_value = "EUR")]
        currency: String,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Merge {
            csv_file_paths,
            currency,
        } => merge_command(csv_file_paths, currency),
    }
}

fn merge_command(csv_file_paths: Vec<PathBuf>, currency: String) -> Result<(), Box<dyn Error>> {
    let mut first_file: Option<PathBuf> = None;
    let mut currency_transactions: SortedSet<CsvTransaction> = SortedSet::new();
    for csv_file_path in csv_file_paths {
        if first_file.is_none() {
            first_file = Some(csv_file_path.clone());
        }
        eprintln!(
            "Parsing CSV file {} filtered by currency {}",
            csv_file_path.as_path().display(),
            currency
        );

        let df_csv = CsvReader::from_path(csv_file_path.clone())?
            .has_header(true)
            .with_try_parse_dates(true)
            .finish()?;

        // PayPal configs
        let expected_columns = ["Date", "Time", "TimeZone", "Name", "Type"];
        let select_columns = ["Date", "Currency", "Gross", "Type", "Name"];
        let source = "PayPal";

        let schema = df_csv.schema();
        let actual_columns: Vec<&str> = schema
            .iter_names()
            .take(expected_columns.len())
            .map(|field| field.as_str())
            .collect();
        println!("Actual column names: {:?}", actual_columns);
        if actual_columns == expected_columns {
            println!("Identical columns")
        }

        let df_filtered = df_csv
            .lazy()
            .filter(col("Currency").eq(lit(currency.to_uppercase().as_str())))
            .filter(col("Balance Impact").eq(lit("Debit")))
            .select([cols(select_columns)])
            .collect()?;

        const DEFAULT_COLUMN_VALUE: AnyValue = AnyValue::String("");
        for row_index in 0..df_filtered.height() {
            let mut row = Row::new(vec![DEFAULT_COLUMN_VALUE; expected_columns.len()]);

            // https://stackoverflow.com/questions/72440403/iterate-over-rows-polars-rust
            df_filtered.get_row_amortized(row_index, &mut row)?;

            let gregorian_days: i32 = row.0[0].try_extract()?;
            let naive_date =
                NaiveDate::from_num_days_from_ce_opt(gregorian_days + EPOCH_DAYS_FROM_CE).unwrap();
            let transaction = CsvTransaction {
                date: naive_date,
                source: source.to_string(),
                currency: row.0[1].to_string(),
                amount: row.0[2].to_string(),
                transaction_type: row.0[3].to_string(),
                payee: row.0[4].to_string(),
            };
            currency_transactions.push(transaction);
        }
        // TODO: Continue from here

        // if header_contains_string(&header, N26Transaction::identification_column()) {
        //     for result in rdr.records() {
        //         let record = result?;
        //
        //         let n26_transaction = record.deserialize::<N26Transaction>(None)?;
        //         if n26_transaction.valid(&currency) {
        //             currency_transactions.push(n26_transaction.to_csv_transaction());
        //         }
        //     }
        // } else if header_contains_string(&header, PayPalTransaction::identification_column()) {
        //     for result in rdr.records() {
        //         let record = result?;
        //
        //         let paypal_transaction = record.deserialize::<PayPalTransaction>(None)?;
        //         if paypal_transaction.valid(&currency) {
        //             currency_transactions.push(paypal_transaction.to_csv_transaction());
        //         }
        //     }
        // } else {
        //     eprintln!("This file is not a N26 CSV.")
        // }
    }

    // Group transactions by year and month
    let mut transaction_map: HashMap<(i32, u32), SortedSet<&CsvTransaction>> = HashMap::new();
    for transaction in currency_transactions.iter() {
        let date = transaction.date;
        let year = date.year();
        let month = date.month();
        let key = (year, month);
        let transactions_for_key = transaction_map.entry(key).or_default();
        transactions_for_key.push(transaction);
    }

    // Sort by year and month
    let mut sorted_keys = transaction_map.keys().collect::<Vec<_>>();
    sorted_keys.sort();

    // Write one CSV per year/month
    for &(year, month) in &sorted_keys {
        let transactions = transaction_map.get(&(*year, *month)).unwrap();
        let year_month_filename = format!(
            "Transactions-{}-{:04}-{:02}.csv",
            currency.to_uppercase(),
            year,
            month
        );
        // Save new CSV files in the same dir of the first file
        let new_path = first_file
            .as_ref()
            .expect("There is no first file")
            .with_file_name(year_month_filename);
        eprintln!("\nWriting output file {}", new_path.as_path().display());
        let mut writer = Writer::from_path(new_path)?;
        writer.write_record(&CsvTransaction::header())?;
        for trn in transactions.iter() {
            println!("{}", trn);
            writer.write_record(&trn.to_record())?;
        }
        writer.flush()?;
    }
    Ok(())
}
