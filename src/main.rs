use bank_csv::{detect_separator, filter_data_frame, CsvOutputRow, NUM_COLUMNS};
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
    let mut currency_transactions: SortedSet<CsvOutputRow> = SortedSet::new();
    let upper_currency = currency.to_uppercase();
    for csv_file_path in csv_file_paths {
        if first_file.is_none() {
            first_file = Some(csv_file_path.clone());
        }
        eprintln!(
            "Parsing CSV file {} filtered by currency {}",
            csv_file_path.as_path().display(),
            upper_currency
        );

        let df_csv: DataFrame;
        match detect_separator(csv_file_path.as_path()) {
            Ok(separator) => {
                df_csv = CsvReader::from_path(csv_file_path.clone())?
                    .has_header(true)
                    .with_try_parse_dates(true)
                    .with_separator(separator)
                    .finish()?;
            }
            Err(err) => {
                eprintln!("{}", err);
                continue;
            }
        };
        let (source, df_filtered) = filter_data_frame(&df_csv, upper_currency.clone());

        const DEFAULT_COLUMN_VALUE: AnyValue = AnyValue::String("");
        let mut row = Row::new(vec![DEFAULT_COLUMN_VALUE; NUM_COLUMNS]);
        for row_index in 0..df_filtered.height() {
            // https://stackoverflow.com/questions/72440403/iterate-over-rows-polars-rust
            df_filtered.get_row_amortized(row_index, &mut row)?;

            let naive_date: NaiveDate;
            match row.0[0].try_extract::<i32>() {
                Ok(gregorian_days) => {
                    naive_date =
                        NaiveDate::from_num_days_from_ce_opt(gregorian_days + EPOCH_DAYS_FROM_CE)
                            .unwrap();
                }
                // Some CSVs hve the date in the German format
                Err(_) => {
                    let date_str = row.0[0].get_str().unwrap();
                    naive_date = NaiveDate::parse_from_str(date_str, "%d.%m.%Y")?;
                }
            }
            let transaction = CsvOutputRow::new(
                naive_date,
                source.to_string(),
                row.0[1].to_string(),
                row.0[2].to_string(),
                row.0[3].to_string(),
                row.0[4].to_string(),
            );
            currency_transactions.push(transaction);
        }
    }

    // Group transactions by year and month
    let mut transaction_map: HashMap<(i32, u32), SortedSet<&CsvOutputRow>> = HashMap::new();
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
            upper_currency, year, month
        );
        // Save new CSV files in the same dir of the first file
        let new_path = first_file
            .as_ref()
            .expect("There is no first file")
            .with_file_name(year_month_filename);
        eprintln!("\nWriting output file {}", new_path.as_path().display());
        let mut writer = Writer::from_path(new_path)?;
        writer.write_record(&CsvOutputRow::header())?;
        for trn in transactions.iter() {
            println!("{}", trn);
            writer.write_record(&trn.to_record())?;
        }
        writer.flush()?;
    }
    Ok(())
}
