//! End-to-end integration tests for the bank-csv CLI
use assert_cmd::Command;
use std::path::Path;

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Parse CSV output bytes into header and records
fn parse_csv_output(stdout: &[u8]) -> (Vec<String>, Vec<Vec<String>>) {
    let mut rdr = csv::Reader::from_reader(stdout);
    let headers: Vec<String> = rdr
        .headers()
        .expect("headers")
        .iter()
        .map(|s| s.to_string())
        .collect();
    let records: Vec<Vec<String>> = rdr
        .records()
        .map(|r| r.expect("record").iter().map(|s| s.to_string()).collect())
        .collect();
    (headers, records)
}

// --- merge command ---

#[test]
fn test_merge_n26_new() {
    let output_dir = std::env::temp_dir().join(format!("bank-csv-test-{}", std::process::id()));
    std::fs::create_dir_all(&output_dir).expect("create temp dir");

    let n26_new = fixture("n26_new.csv");

    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("merge")
        .arg(n26_new.to_str().unwrap())
        .arg("--output-dir")
        .arg(output_dir.to_str().unwrap());

    cmd.assert().success();

    // Find the output CSV file
    let entries: Vec<_> = std::fs::read_dir(&output_dir)
        .expect("read output dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "csv")
                .unwrap_or(false)
        })
        .collect();

    assert_eq!(entries.len(), 1, "Expected exactly one output CSV file");

    let output_path = entries[0].path();
    let content = std::fs::read_to_string(&output_path).expect("read output csv");
    let mut lines = content.lines();

    // Check header
    let header = lines.next().expect("expected header line");
    assert!(
        header.contains("Date"),
        "Header should contain Date: {}",
        header
    );
    assert!(
        header.contains("Source"),
        "Header should contain Source: {}",
        header
    );
    assert!(
        header.contains("Currency"),
        "Header should contain Currency: {}",
        header
    );
    assert!(
        header.contains("Amount"),
        "Header should contain Amount: {}",
        header
    );

    // Count data rows (n26_new has 2 rows)
    let data_rows: Vec<_> = lines.filter(|l| !l.is_empty()).collect();
    assert_eq!(data_rows.len(), 2, "Expected 2 data rows");

    // Clean up
    std::fs::remove_dir_all(&output_dir).ok();
}

// --- passthrough command ---

#[test]
fn test_passthrough_n26_new() {
    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough").arg(fixture("n26_new.csv"));

    let output = cmd.assert().success().get_output().stdout.clone();
    let (headers, records) = parse_csv_output(&output);

    assert_eq!(
        headers,
        vec![
            "Date",
            "Source",
            "Currency",
            "Amount",
            "Type",
            "Payee",
            "Memo",
            "BankId",
            "OriginalAmount",
            "OriginalCurrency"
        ],
        "Header should be the 10-column passthrough schema"
    );
    // n26_new has 3 data rows (2 EUR + 1 BRL FX)
    assert_eq!(records.len(), 3);
    // All amounts should be dot-decimal
    for row in &records {
        let amount = &row[3];
        assert!(
            regex_dot_decimal(amount),
            "Amount should be dot-decimal: {}",
            amount
        );
        // ISO 8601 date
        assert!(
            regex_iso_date(&row[0]),
            "Date should be ISO 8601: {}",
            row[0]
        );
        // N26 has no BankId
        assert_eq!(row[7], "", "N26 should have empty BankId");
    }
}

#[test]
fn test_passthrough_n26_old() {
    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough").arg(fixture("n26_old.csv"));

    let output = cmd.assert().success().get_output().stdout.clone();
    let (headers, records) = parse_csv_output(&output);

    assert_eq!(headers[0], "Date");
    assert_eq!(headers[7], "BankId");
    assert_eq!(records.len(), 2);
    for row in &records {
        assert!(regex_dot_decimal(&row[3]), "Amount dot-decimal: {}", row[3]);
        assert!(regex_iso_date(&row[0]), "Date ISO 8601: {}", row[0]);
        assert_eq!(row[7], "", "N26 BankId should be empty");
    }
}

#[test]
fn test_passthrough_dkb_old() {
    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough").arg(fixture("dkb_old.csv"));

    let output = cmd.assert().success().get_output().stdout.clone();
    let (headers, records) = parse_csv_output(&output);

    assert_eq!(headers[0], "Date");
    assert_eq!(headers[7], "BankId");
    assert_eq!(records.len(), 2);
    // DKB: BankId should be non-empty for at least one row
    let has_bank_id = records.iter().any(|r| !r[7].is_empty());
    assert!(has_bank_id, "DKB should have at least one row with BankId");
    // First row has 485210393446368
    assert!(
        records.iter().any(|r| r[7] == "485210393446368"),
        "DKB old should have 485210393446368 as BankId"
    );
    for row in &records {
        assert!(regex_dot_decimal(&row[3]), "Amount dot-decimal: {}", row[3]);
    }
}

#[test]
fn test_passthrough_dkb_new() {
    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough").arg(fixture("dkb_new.csv"));

    let output = cmd.assert().success().get_output().stdout.clone();
    let (_headers, records) = parse_csv_output(&output);

    assert_eq!(records.len(), 2);
    // DKB new: BankId from Kundenreferenz
    let has_485 = records.iter().any(|r| r[7] == "485210393446001");
    assert!(
        has_485,
        "DKB new should have 485210393446001 as BankId: {:?}",
        records.iter().map(|r| r[7].clone()).collect::<Vec<_>>()
    );
}

#[test]
fn test_passthrough_paypal_current() {
    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough").arg(fixture("paypal_current.csv"));

    let output = cmd.assert().success().get_output().stdout.clone();
    let (_headers, records) = parse_csv_output(&output);

    // PayPal fixture: 2 Debit EUR rows (1 Credit/USD filtered)
    assert_eq!(records.len(), 2, "Expected 2 rows after PayPal filtering");
    // BankId values match fixture Transaction IDs
    assert!(
        records.iter().any(|r| r[7] == "TX-FAKE-001"),
        "Should have TX-FAKE-001"
    );
    assert!(
        records.iter().any(|r| r[7] == "TX-FAKE-002"),
        "Should have TX-FAKE-002"
    );
    // No General Currency Conversion row
    for row in &records {
        assert_ne!(
            row[4], "General Currency Conversion",
            "GCC row should be filtered"
        );
    }
    for row in &records {
        assert!(regex_dot_decimal(&row[3]), "Amount dot-decimal: {}", row[3]);
    }
}

#[test]
fn test_passthrough_paypal_old() {
    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough").arg(fixture("paypal_old.csv"));

    let output = cmd.assert().success().get_output().stdout.clone();
    let (_headers, records) = parse_csv_output(&output);

    // paypal_old has 1 Shopping row (EUR), 1 GCC row (USD, filtered)
    assert_eq!(
        records.len(),
        1,
        "Expected 1 row after PayPal old filtering"
    );
    assert_eq!(records[0][7], "TX-OLD-001", "BankId should be TX-OLD-001");
}

#[test]
fn test_passthrough_multi_file() {
    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough")
        .arg(fixture("n26_new.csv"))
        .arg(fixture("paypal_current.csv"));

    let output = cmd.assert().success().get_output().stdout.clone();
    let (headers, records) = parse_csv_output(&output);

    // Exactly ONE header line (handled by csv::Writer)
    assert_eq!(headers.len(), 10, "Should have 10 header fields");
    // n26_new=3 rows + paypal_current=2 rows
    assert_eq!(records.len(), 5, "Expected 5 total rows (3 N26 + 2 PayPal)");
}

#[test]
fn test_passthrough_unknown_format() {
    let temp = tempfile::NamedTempFile::new().expect("tempfile");
    std::fs::write(temp.path(), "Foo,Bar,Baz\n1,2,3\n").expect("write temp");

    let mut cmd = Command::cargo_bin("bank-csv").expect("binary not found");
    cmd.arg("passthrough").arg(temp.path());

    let output = cmd.assert().failure().get_output().stderr.clone();
    let stderr = String::from_utf8_lossy(&output);
    assert!(
        stderr.contains(temp.path().to_string_lossy().as_ref()),
        "Error message should contain file path: {}",
        stderr
    );
}

/// Check if a string matches dot-decimal amount format (e.g. -23.45 or 2000.00)
fn regex_dot_decimal(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    let s = s.strip_prefix('-').unwrap_or(s);
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 2 {
        return false;
    }
    parts[0].chars().all(|c| c.is_ascii_digit()) && parts[1].chars().all(|c| c.is_ascii_digit())
}

/// Check if a string matches ISO 8601 date format (YYYY-MM-DD)
fn regex_iso_date(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return false;
    }
    parts[0].len() == 4
        && parts[1].len() == 2
        && parts[2].len() == 2
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && parts[1].chars().all(|c| c.is_ascii_digit())
        && parts[2].chars().all(|c| c.is_ascii_digit())
}
