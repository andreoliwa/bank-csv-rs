//! Detect CSV files from a couple of German banks (N26, DKB) and PayPal,
//! filter out transactions in a specific currency and generate a CSV file with these transactions
use bank_csv::{detect_separator, dkb_edit_file, filter_data_frame, CsvOutputRow, Source};
use chrono::Datelike;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use csv::Writer;
use std::collections::HashMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate shell completion scripts
    Completions {
        /// Shell to generate completions for
        shell: Shell,
    },
    /// Merge one or more bank CSV files and split them into multiple files, one for each month
    #[command(arg_required_else_help = true)]
    Merge {
        /// Path(s) to the CSV file(s) to be parsed
        csv_file_paths: Vec<PathBuf>,
        /// Currency to filter (case-insensitive)
        #[arg(short, long, default_value = "EUR")]
        currency: String,
        /// Output directory to generate the CSV files. Default: download directory
        #[arg(short, long, value_hint = clap::ValueHint::DirPath)]
        output_dir: Option<PathBuf>,
    },
    /// Read one or more bank CSV files and emit a normalized intermediate CSV to stdout
    #[command(arg_required_else_help = true)]
    Passthrough {
        /// Path(s) to the CSV file(s) to be parsed
        csv_file_paths: Vec<PathBuf>,
    },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Completions { shell } => {
            generate(
                shell,
                &mut Cli::command(),
                "bank-csv",
                &mut std::io::stdout(),
            );
            Ok(())
        }
        Commands::Merge {
            csv_file_paths,
            currency,
            output_dir,
        } => merge_command(csv_file_paths, currency, output_dir),
        Commands::Passthrough { csv_file_paths } => passthrough_command(csv_file_paths),
    }
}

fn merge_command(
    csv_file_paths: Vec<PathBuf>,
    currency: String,
    original_output_dir: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let output_dir: PathBuf = match original_output_dir {
        None => dirs::download_dir().unwrap(),
        Some(output_dir) => {
            PathBuf::from(shellexpand::tilde(&output_dir.to_string_lossy()).to_string())
        }
    };
    if !output_dir.exists() {
        return Err(format!(
            "Output directory {} does not exist",
            output_dir.as_path().display()
        )
        .into());
    }
    if !output_dir.is_dir() {
        return Err(format!(
            "Output directory {} is not a directory",
            output_dir.as_path().display()
        )
        .into());
    }

    let mut currency_transactions: Vec<CsvOutputRow> = Vec::new();
    let upper_currency = currency.to_uppercase();
    for original_path in csv_file_paths {
        let expanded_path =
            PathBuf::from(shellexpand::tilde(&original_path.to_string_lossy()).to_string());
        if !expanded_path.exists() {
            eprintln!(
                "CSV file {} does not exist",
                expanded_path.as_path().display()
            );
            continue;
        }
        eprintln!(
            "Parsing CSV file {} filtered by currency {}",
            expanded_path.as_path().display(),
            upper_currency
        );

        let (separator, source) = match detect_separator(expanded_path.as_path()) {
            Ok(result) => result,
            Err(err) => {
                eprintln!("{}", err);
                continue;
            }
        };
        let temp_file = NamedTempFile::new()?;
        let modified_path: &Path = match source {
            Some(Source::DKB) => {
                dkb_edit_file(expanded_path.as_path(), &temp_file)?;
                temp_file.path()
            }
            _ => expanded_path.as_path(),
        };

        match filter_data_frame(modified_path, separator, &upper_currency) {
            Ok((_source, rows)) => {
                for row in rows {
                    currency_transactions.push(row);
                }
            }
            Err(err) => {
                eprintln!("{}", err);
                continue;
            }
        }
    }

    // Sort all transactions
    currency_transactions.sort();

    // Group transactions by year and month
    let mut transaction_map: HashMap<(i32, u32), Vec<&CsvOutputRow>> = HashMap::new();
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
            "bank-csv-transactions-{}-{:04}-{:02}.csv",
            upper_currency, year, month
        );
        let mut new_path = output_dir.clone();
        new_path.push(year_month_filename);
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

fn passthrough_command(csv_file_paths: Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    let mut writer = csv::Writer::from_writer(std::io::stdout());
    writer.write_record(&CsvOutputRow::header())?;
    let mut had_error = false;

    for original_path in csv_file_paths {
        let expanded_path =
            PathBuf::from(shellexpand::tilde(&original_path.to_string_lossy()).to_string());
        if !expanded_path.exists() {
            eprintln!("{}: file does not exist", expanded_path.display());
            had_error = true;
            continue;
        }
        eprintln!("Parsing {}", expanded_path.display());

        let (separator, source_hint) = match detect_separator(expanded_path.as_path()) {
            Ok(result) => result,
            Err(err) => {
                eprintln!("{}: {}", expanded_path.display(), err);
                had_error = true;
                continue;
            }
        };
        let temp_file = NamedTempFile::new()?;
        let modified_path: &Path = match source_hint {
            Some(Source::DKB) => {
                dkb_edit_file(expanded_path.as_path(), &temp_file)?;
                temp_file.path()
            }
            _ => expanded_path.as_path(),
        };

        let rows = match filter_data_frame(modified_path, separator, "EUR") {
            Ok((_source, rows)) => rows,
            Err(err) => {
                eprintln!("{}: {}", expanded_path.display(), err);
                had_error = true;
                continue;
            }
        };
        for row in rows {
            writer.write_record(&row.to_passthrough_record())?;
        }
    }
    writer.flush()?;
    if had_error {
        return Err("One or more files failed to parse".into());
    }
    Ok(())
}
