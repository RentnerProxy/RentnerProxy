use super::*;
use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};

struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("log capture poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn reports_failed_recovery_with_a_bounded_code_and_operation() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&output);
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_target(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || Capture(Arc::clone(&writer)))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        report_recovery_result(Ok(()), "successful_write");
        report_recovery_result(Err(CertificateError::StoreUnavailable), "persist_index");
    });
    let bytes = output.lock().expect("log capture was not poisoned").clone();
    let logged = String::from_utf8(bytes).expect("tracing emits UTF-8");
    assert!(logged.contains("certificate_recovery_operation_failed"));
    assert!(logged.contains("operation=\"persist_index\""));
    assert!(logged.contains("error_code=\"certificate_store_unavailable\""));
    assert!(!logged.contains("successful_write"));
}
