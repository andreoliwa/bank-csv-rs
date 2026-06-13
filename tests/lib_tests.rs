//! Unit tests for lib.rs functions: filter_data_frame, detect_separator, dkb_edit_file, dkb_extract_amount
use bank_csv::{detect_separator, dkb_edit_file, dkb_extract_amount, filter_data_frame, Source};
use chrono::NaiveDate;
use std::path::Path;
use tempfile::NamedTempFile;

fn fixture(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

// --- detect_separator ---

#[test]
fn test_detect_separator_dkb_old() {
    let path = fixture("dkb_old.csv");
    let (sep, source) = detect_separator(&path).expect("detect_separator failed");
    assert_eq!(sep, b';');
    assert_eq!(source, Some(Source::DKB));
}

#[test]
fn test_detect_separator_n26_old() {
    let path = fixture("n26_old.csv");
    let (sep, source) = detect_separator(&path).expect("detect_separator failed");
    assert_eq!(sep, b',');
    assert!(source.is_none());
}

#[test]
fn test_detect_separator_paypal_current() {
    let path = fixture("paypal_current.csv");
    let (sep, source) = detect_separator(&path).expect("detect_separator failed");
    assert_eq!(sep, b',');
    assert!(source.is_none());
}

// --- dkb_extract_amount ---

#[test]
fn test_dkb_extract_amount_found() {
    let result = dkb_extract_amount("BRL", "Original 6,99 BRL 1 Euro=5.29 BRL");
    assert_eq!(result, Some("6,99".to_string()));
}

#[test]
fn test_dkb_extract_amount_not_found() {
    let result = dkb_extract_amount("BRL", "Nothing here");
    assert_eq!(result, None);
}

// --- dkb_edit_file ---

#[test]
fn test_dkb_edit_file_strips_header_lines() {
    let path = fixture("dkb_old.csv");
    let temp = NamedTempFile::new().expect("tempfile failed");
    dkb_edit_file(&path, &temp).expect("dkb_edit_file failed");

    let content = std::fs::read_to_string(temp.path()).expect("read temp file failed");
    // Should start with the actual data header (Verwendungszweck is in it)
    assert!(
        content.starts_with('"'),
        "Should start with quoted header field"
    );
    assert!(
        content.contains("Buchungstag"),
        "Should contain Buchungstag header"
    );
    // Should NOT contain the metadata lines
    assert!(
        !content.contains("Girokonto"),
        "Should not contain the account metadata"
    );
}

// --- filter_data_frame ---

#[test]
fn test_filter_data_frame_dkb_old() {
    let path = fixture("dkb_old.csv");
    let temp = NamedTempFile::new().expect("tempfile failed");
    dkb_edit_file(&path, &temp).expect("dkb_edit_file failed");

    let (source, rows) =
        filter_data_frame(temp.path(), b';', "EUR").expect("filter_data_frame failed");
    assert_eq!(source, Source::DKB);
    // Both rows are EUR - DKB old fixture has 2 data rows
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_filter_data_frame_n26_old() {
    let path = fixture("n26_old.csv");
    let (source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    assert_eq!(source, Source::N26);
    // N26 old fixture: 2 rows, both EUR (empty currency = EUR)
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_filter_data_frame_paypal_current_debit_only() {
    let path = fixture("paypal_current.csv");
    let (source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    assert_eq!(source, Source::PayPal);
    // PayPal current fixture: 3 rows total; Credit + General Currency Conversion filtered
    // Only 2 Debit EUR rows remain
    assert_eq!(rows.len(), 2);
    // Verify no "General Currency Conversion" row is present
    for row in &rows {
        assert_ne!(
            row.transaction_type, "General Currency Conversion",
            "General Currency Conversion row should be filtered"
        );
    }
    // BankId assertions: PayPal rows carry Transaction ID
    assert_eq!(rows[0].bank_id, "TX-FAKE-001");
    assert_eq!(rows[1].bank_id, "TX-FAKE-002");
    // Date regression: fixture uses DD/MM/YYYY (DE locale)
    assert_eq!(rows[0].date, NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    assert_eq!(rows[1].date, NaiveDate::from_ymd_opt(2024, 1, 20).unwrap());
}

#[test]
fn test_filter_data_frame_dkb_old_bank_id() {
    let path = fixture("dkb_old.csv");
    let temp = NamedTempFile::new().expect("tempfile failed");
    dkb_edit_file(&path, &temp).expect("dkb_edit_file failed");

    let (_source, rows) =
        filter_data_frame(temp.path(), b';', "EUR").expect("filter_data_frame failed");
    assert_eq!(rows[0].bank_id, "485210393446368");
    assert_eq!(rows[1].bank_id, "SEPAREF123456");
}

#[test]
fn test_filter_data_frame_n26_bank_id_empty() {
    let path = fixture("n26_old.csv");
    let (_source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    for row in &rows {
        assert_eq!(row.bank_id, "", "N26 rows should always have empty bank_id");
    }
}

// --- PayPal payee and memo construction ---

#[test]
fn test_paypal_payee_name_and_email() {
    let path = fixture("paypal_payee_memo.csv");
    let (_source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    assert_eq!(rows[0].payee, "Alice Musterfrau <seller@example.com>");
}

#[test]
fn test_paypal_payee_email_only() {
    let path = fixture("paypal_payee_memo.csv");
    let (_source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    assert_eq!(rows[2].payee, "anon@example.com");
}

#[test]
fn test_paypal_payee_name_only() {
    let path = fixture("paypal_payee_memo.csv");
    let (_source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    assert_eq!(rows[3].payee, "Carol Nomail");
}

#[test]
fn test_paypal_memo_prefers_note_over_subject() {
    let path = fixture("paypal_payee_memo.csv");
    let (_source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    assert_eq!(rows[0].memo, "note text");
}

#[test]
fn test_paypal_memo_falls_back_to_subject() {
    let path = fixture("paypal_payee_memo.csv");
    let (_source, rows) = filter_data_frame(&path, b',', "EUR").expect("filter_data_frame failed");
    assert_eq!(rows[1].memo, "subject text");
}
