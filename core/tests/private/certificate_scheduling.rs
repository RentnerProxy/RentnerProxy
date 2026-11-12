use super::*;

#[test]
fn retry_deadlines_preserve_later_backoff_and_replace_absent_or_invalid_state() {
    let earlier = "2026-11-12T12:00:00Z";
    let later = "2026-11-12T13:00:00Z";
    assert_eq!(max_retry_deadline_string(Some(later), earlier), later);
    assert_eq!(max_retry_deadline_string(Some(earlier), later), later);
    assert_eq!(max_retry_deadline_string(Some(later), later), later);
    assert_eq!(max_retry_deadline_string(None, later), later);
    assert_eq!(max_retry_deadline_string(Some("invalid"), later), later);
}
