# bank-csv

Handle CSV files from a few German banks and PayPal.

## Installation

Clone the project and run `make install` to install the package.

This package is not yet published on [crates.io](https://crates.io/).

## Usage

Merge CSV files from a few German banks and PayPal into a single CSV file.

```bash
bank-csv merge ~/Downloads/import-*.csv
```

This will generate `Transactions*.csv` files in the `~/Downloads` directory, with transactions sorted by date and grouped by month.

Type `bank-csv --help` for more details.

## Supported CSV files

| Bank                                     | Where to get the CSV                                                                      |
|------------------------------------------|-------------------------------------------------------------------------------------------|
| [N26](https://n26.com/)                  | [Downloads](https://app.n26.com/downloads)                                                |
| [PayPal](https://www.paypal.com/)        | [Activity report](https://www.paypal.com/reports/dlog)                                    |
| (coming soon) [DKB](https://www.dkb.de/) | [DKB Konto Umsätze](https://www.ib.dkb.de/banking/finanzstatus/kontoumsaetze?$event=init) | 

PayPal has (as I found so far) 2 different CSV file formats.

This project uses [polars](https://github.com/pola-rs/polars) to read CSV files directly by column names.
It's a heavier dependency, but it's easier to support different CSV formats without being super strict about column order and presence.  

## Roadmap (TODO)

- [ ] Add support for DKB CSV files
- [ ] Generate OFX (or QIF) files to be imported into [GnuCash](https://www.gnucash.org/)
