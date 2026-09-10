use std::sync::LazyLock;

use regex::Regex;

use super::Region;

/// Matches high-entropy alphanumeric strings (10+ chars).
/// Excludes `/` to avoid matching file paths as single tokens.
static SECRET_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[A-Za-z0-9+_=-]{10,}").expect("hardcoded regex should compile"));

const ENTROPY_THRESHOLD: f64 = 4.5;

/// Compute Shannon entropy (bits per byte) of a string.
pub(super) fn shannon_entropy(s: &str) -> f64 {
    if s.is_empty() {
        return 0.0;
    }
    let mut freq = [0u32; 256];
    for &b in s.as_bytes() {
        freq[b as usize] += 1;
    }
    let len = s.len() as f64;
    let mut entropy = 0.0;
    for &count in &freq {
        if count > 0 {
            let p = f64::from(count) / len;
            entropy -= p * p.log2();
        }
    }
    entropy
}

/// Find high-entropy alphanumeric tokens in `s`.
///
/// Returns regions where tokens match `[A-Za-z0-9+_=-]{10,}` and have
/// Shannon entropy above the threshold (4.5 bits). Protects against
/// consuming characters from JSON escape sequences.
///
/// An assignment token (`NAME=value`) is measured and redacted by its
/// value alone. Measuring the pair merges the name's charset into the
/// value's and pushes innocuous values (a pure-hex git SHA can never
/// exceed 4.0 bits by itself) over the threshold, and redacting the
/// pair destroys the name that says what was redacted.
pub(super) fn find_entropy_regions(s: &str) -> Vec<Region> {
    let mut regions = Vec::new();
    for m in SECRET_PATTERN.find_iter(s) {
        let mut start = m.start();
        let end = m.end();

        // Protect against consuming characters from JSON escape sequences.
        // E.g. in "controller.go\nmodel.go", regex could match "nmodel"
        // (consuming 'n' from '\n'). Skip the escape character to avoid
        // creating invalid escape sequences after replacement.
        if start > 0 && s.as_bytes()[start - 1] == b'\\' {
            match s.as_bytes()[start] {
                b'n' | b't' | b'r' | b'b' | b'f' | b'u' | b'"' | b'\\' | b'/' => {
                    start += 1;
                    if end - start < 10 {
                        continue;
                    }
                }
                _ => {}
            }
        }

        if let Some(offset) = assignment_value_offset(&s[start..end]) {
            start += offset;
        }

        if shannon_entropy(&s[start..end]) > ENTROPY_THRESHOLD {
            regions.push(Region { start, end });
        }
    }
    regions
}

/// For an assignment token (`NAME=value` with an identifier-shaped name),
/// return the byte offset where the value begins. Entropy above the 4.5-bit
/// threshold needs at least 23 distinct characters, so a value too short to
/// qualify simply measures under the threshold; no length guard is needed.
fn assignment_value_offset(token: &str) -> Option<usize> {
    let eq = token.find('=')?;
    let value = &token[eq + 1..];
    if value.is_empty() || value.starts_with('=') {
        return None;
    }
    let name = &token[..eq];
    let mut chars = name.chars();
    let first = chars.next()?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    Some(eq + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entropy_empty_string() {
        assert!(shannon_entropy("").abs() < f64::EPSILON);
    }

    #[test]
    fn entropy_single_char_repeated() {
        assert!(shannon_entropy("aaaa").abs() < f64::EPSILON);
    }

    #[test]
    fn entropy_two_equal_chars() {
        let e = shannon_entropy("ab");
        assert!((e - 1.0).abs() < 0.001, "expected ~1.0, got {e}");
    }

    #[test]
    fn entropy_aws_key_above_3() {
        let e = shannon_entropy("AKIAIOSFODNN7EXAMPLE");
        assert!(e > 3.0, "expected > 3.0, got {e}");
    }

    #[test]
    fn regions_empty_for_normal_text() {
        assert!(find_entropy_regions("hello world").is_empty());
    }

    #[test]
    fn regions_finds_high_entropy_token() {
        // "key=xK9..." matches as one token, but only the value is
        // measured and flagged; the name survives redaction.
        let input = "key=xK9mZ2vL8nQ5rT1wY4bC7dF0gH3jE6p";
        let regions = find_entropy_regions(input);
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].start, "key=".len());
        assert_eq!(regions[0].end, input.len());
    }

    #[test]
    fn regions_find_padded_base64_tokens() {
        for input in [
            "WxFhjC5EAnh30M0JIe0Wa58Xb1BYf8kedTTdKUbbd9Y=",
            "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789ABCDEF==",
        ] {
            assert_eq!(find_entropy_regions(input), vec![Region {
                start: 0,
                end:   input.len(),
            }]);
        }
    }

    #[test]
    fn regions_empty_for_json_escape_sequence() {
        // "controller.go\nmodel.go" — the regex could match across the \n boundary
        let regions = find_entropy_regions(r"controller.go\nmodel.go");
        assert!(regions.is_empty(), "got regions: {regions:?}");
    }

    #[test]
    fn regions_empty_for_file_path() {
        // / is excluded from the pattern, so path segments are short
        let regions = find_entropy_regions("/tmp/test/controller.go");
        assert!(regions.is_empty(), "got regions: {regions:?}");
    }
}
