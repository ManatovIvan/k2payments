// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use subtle::ConstantTimeEq;

/// Parse a Bearer token from an `Authorization` header value.
///
/// Expects the header to be in the form `Bearer <token>`. Returns `None` if the
/// header is missing, malformed, or does not use the Bearer scheme.
pub fn parse_bearer_token(header: Option<&str>) -> Option<&str> {
    let value = header?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    Some(token)
}

/// Constant-time equality comparison for two strings.
///
/// This is intended for comparing secrets (e.g. bearer tokens) without
/// leaking timing information about the prefix.
pub fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let max_len = left.len().max(right.len());

    let mut left_padded = vec![0_u8; max_len];
    let mut right_padded = vec![0_u8; max_len];
    left_padded[..left.len()].copy_from_slice(left);
    right_padded[..right.len()].copy_from_slice(right);

    let content_eq = left_padded.ct_eq(&right_padded);
    let len_eq = (left.len() as u64).ct_eq(&(right.len() as u64));
    bool::from(content_eq & len_eq)
}

#[cfg(test)]
mod tests {
    use super::{constant_time_eq, parse_bearer_token};

    #[test]
    fn parse_bearer_token_extracts_token() {
        assert_eq!(
            parse_bearer_token(Some("Bearer secret-token")),
            Some("secret-token")
        );
    }

    #[test]
    fn parse_bearer_token_is_case_insensitive() {
        assert_eq!(
            parse_bearer_token(Some("bearer lower-case")),
            Some("lower-case")
        );
        assert_eq!(
            parse_bearer_token(Some("BEARER UPPER-CASE")),
            Some("UPPER-CASE")
        );
    }

    #[test]
    fn parse_bearer_token_rejects_missing_header() {
        assert_eq!(parse_bearer_token(None), None);
    }

    #[test]
    fn parse_bearer_token_rejects_missing_scheme() {
        assert_eq!(parse_bearer_token(Some("secret-token")), None);
    }

    #[test]
    fn parse_bearer_token_rejects_wrong_scheme() {
        assert_eq!(parse_bearer_token(Some("Basic secret-token")), None);
    }

    #[test]
    fn parse_bearer_token_rejects_empty_token() {
        assert_eq!(parse_bearer_token(Some("Bearer  ")), None);
    }

    #[test]
    fn constant_time_eq_accepts_equal_strings() {
        assert!(constant_time_eq("same", "same"));
    }

    #[test]
    fn constant_time_eq_rejects_different_strings() {
        assert!(!constant_time_eq("one", "two"));
    }

    #[test]
    fn constant_time_eq_rejects_mismatched_lengths() {
        assert!(!constant_time_eq("short", "a-very-different-length"));
    }
}
