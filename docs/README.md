# bank-csv

Handle CSV files from a few German banks and PayPal.

## Supported CSV files

| Bank                              | Where to get the CSV                                                                      |
| --------------------------------- | ----------------------------------------------------------------------------------------- |
| [N26](https://n26.com/)           | [Downloads](https://app.n26.com/downloads)                                                |
| [PayPal](https://www.paypal.com/) | [Activity report](https://www.paypal.com/reports/dlog)                                    |
| [DKB](https://www.dkb.de/)        | [DKB Konto Umsätze](https://www.ib.dkb.de/banking/finanzstatus/kontoumsaetze?$event=init) |

PayPal CSV columns can be configured on download and the default columns can change.

CSV column detection uses exact header name matching, supporting all documented format
variants for each bank without requiring a specific column order.

## Installation

| From                                           | Command                                                         |
| ---------------------------------------------- | --------------------------------------------------------------- |
| [crates.io](https://crates.io/crates/bank-csv) | `cargo install bank-csv`                                        |
| GitHub                                         | `cargo install --git https://github.com/andreoliwa/bank-csv-rs` |

## Commands

```
bank-csv <COMMAND>

Commands:
  completions  Generate shell completion scripts
  merge        Merge one or more bank CSV files and split them into multiple files, one for each month
  passthrough  Read one or more bank CSV files and emit a normalized intermediate CSV to stdout
  help         Print this message or the help of the given subcommand(s)
```

### merge

Merge CSV files from a few German banks and PayPal into a single CSV file.

```bash
bank-csv merge /path/to/import-*.csv
```

`EUR` transactions are filtered by default. You can choose a different currency with the `--currency` option.

```bash
bank-csv merge -c USD /path/to/import-*.csv
```

This will generate `bank-csv-transactions*.csv` files in the download directory of the computer, with transactions sorted by date and grouped by month.

Output format uses comma-decimal amounts for compatibility with macOS Numbers.

### passthrough

Read one or more bank CSV files and emit a normalized intermediate CSV to stdout.

```bash
bank-csv passthrough /path/to/import-2025-01-n26.csv
```

Output schema (8 columns, comma-separated, dot-decimal amounts, ISO 8601 dates):

| Column   | Description                                   |
| -------- | --------------------------------------------- |
| Date     | Transaction date (YYYY-MM-DD)                 |
| Source   | Bank name (N26, DKB, PayPal)                  |
| Currency | 3-letter ISO currency code                    |
| Amount   | Transaction amount, dot-decimal (e.g. -23.45) |
| Type     | Transaction type from original CSV            |
| Payee    | Counterparty name                             |
| Memo     | Description / reference                       |
| BankId   | Bank-native transaction ID when available     |

BankId population by bank:

- PayPal: Transaction ID column (always populated)
- DKB: Kundenreferenz column (populated for VISA and SEPA transactions, empty otherwise)
- N26: always empty (no native transaction ID column)

Multiple files are merged into one output (one header row, files in argument order):

```bash
bank-csv passthrough jan.csv feb.csv mar.csv
```

## Roadmap (TODO)

- [ ] Generate OFX (or QIF) files to be imported into [GnuCash](https://www.gnucash.org/)
