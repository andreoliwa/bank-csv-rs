# bank-csv

Handle CSV files from a few German banks and PayPal.

## Supported CSV files

| Bank                              | Where to get the CSV                                                                      |
| --------------------------------- | ----------------------------------------------------------------------------------------- |
| [N26](https://n26.com/)           | [Downloads](https://app.n26.com/downloads)                                                |
| [PayPal](https://www.paypal.com/) | [Activity report](https://www.paypal.com/reports/dlog)                                    |
| [DKB](https://www.dkb.de/)        | [DKB Konto Umsätze](https://www.ib.dkb.de/banking/finanzstatus/kontoumsaetze?$event=init) |

PayPal CSV columns can be configured on download and the default columns can change.

This project uses [polars](https://github.com/pola-rs/polars) to read CSV files directly by column names.
It's a heavier dependency, but it's easier to support different CSV formats without being super strict about column order and presence.

## Installation

| From                                           | Command                                                         |
| ---------------------------------------------- | --------------------------------------------------------------- |
| [crates.io](https://crates.io/crates/bank-csv) | `cargo install bank-csv`                                        |
| GitHub                                         | `cargo install --git https://github.com/andreoliwa/bank-csv-rs` |

## Usage

Merge CSV files from a few German banks and PayPal into a single CSV file.

```bash
bank-csv merge /path/to/import-*.csv
```

`EUR` transactions are filtered by default. You can choose a different currency with the `--currency` option.

```bash
bank-csv merge -c USD /path/to/import-*.csv
```

This will generate `bank-csv-transactions*.csv` files in the download directory of the computer, with transactions sorted by date and grouped by month.

Type `bank-csv --help` for more details.

```bash
❯ bank-csv --help
Handle CSV files from a few German banks and PayPal

Usage: bank-csv <COMMAND>

Commands:
  merge  Merge one or more bank CSV files and split them into multiple files, one for each month
  help   Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

## Roadmap (TODO)

- [ ] Generate OFX (or QIF) files to be imported into [GnuCash](https://www.gnucash.org/)
