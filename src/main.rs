use bank_csv::{
    header_contains_string, BaseTransaction, CsvTransaction, N26Transaction, PayPalTransaction,
};
use chrono::Datelike;
use clap::{Parser, Subcommand};
use csv::{Reader, Writer};
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

        // Create a CSV reader and iterate over the rows
        let mut rdr = Reader::from_path(csv_file_path.clone())?;
        let header = rdr.headers()?.clone();

        // TODO: this attempt didn't work, think of a better way to remove duplicated code
        // if !parse_csv_as::<N26Transaction>(
        //     &currency,
        //     &mut currency_transactions,
        //     &mut rdr,
        //     &header,
        // )? {
        //     if !parse_csv_as::<PayPalTransaction>(
        //         &currency,
        //         &mut currency_transactions,
        //         &mut rdr,
        //         &header,
        //     )? {
        //         eprintln!("This file is not a N26 or PayPal CSV.")
        //     }
        // }
        if header_contains_string(&header, N26Transaction::identification_column()) {
            for result in rdr.records() {
                let record = result?;

                let n26_transaction = record.deserialize::<N26Transaction>(None)?;
                if n26_transaction.valid(&currency) {
                    currency_transactions.push(n26_transaction.to_csv_transaction());
                }
            }
        } else if header_contains_string(&header, PayPalTransaction::identification_column()) {
            for result in rdr.records() {
                let record = result?;

                let paypal_transaction = record.deserialize::<PayPalTransaction>(None)?;
                if paypal_transaction.valid(&currency) {
                    currency_transactions.push(paypal_transaction.to_csv_transaction());
                }
            }
        } else {
            eprintln!("This file is not a N26 CSV.")
        }
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
