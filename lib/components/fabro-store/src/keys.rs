use std::fmt::{self, Write};
#[cfg(test)]
use std::ops::Range;

use fabro_types::RunId;

pub(crate) const MAX_EVENT_SEQ: u32 = 999_999;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SlateKey(String);

impl SlateKey {
    const SEP: char = '\0';

    pub(crate) fn new(segment: impl fmt::Display) -> Self {
        Self(segment.to_string())
    }

    pub(crate) fn with(mut self, segment: impl fmt::Display) -> Self {
        self.0.push(Self::SEP);
        write!(&mut self.0, "{segment}").expect("write to String cannot fail");
        self
    }

    pub(crate) fn into_prefix(mut self) -> Self {
        self.0.push(Self::SEP);
        self
    }

    /// Exclusive end bound of this key's prefix keyspace: every key under
    /// `self.into_prefix()` sorts below it and no other key sorts between.
    #[cfg(test)]
    fn into_prefix_end(mut self) -> Self {
        self.0.push('\u{1}');
        self
    }

    #[cfg(test)]
    fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn segments(raw: &str) -> impl Iterator<Item = &str> {
        raw.split(Self::SEP)
    }
}

impl AsRef<[u8]> for SlateKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

// --- Construction ---

pub(crate) fn run_events_prefix(run_id: &RunId) -> SlateKey {
    SlateKey::new("runs")
        .with(run_id)
        .with("events")
        .into_prefix()
}

/// Prefix of the retired `runs/_index/by-start/<run_id>` catalog markers that
/// the legacy layout kept beside each run's events.
pub(crate) fn run_catalog_prefix() -> SlateKey {
    run_catalog_root().into_prefix()
}

#[cfg(test)]
pub(crate) fn run_catalog_key(run_id: &RunId) -> SlateKey {
    run_catalog_root().with(run_id)
}

/// Extracts the run id from a retired `runs/_index/by-start` catalog marker.
///
/// Two shapes exist in the wild and both are canonical:
///
/// * `runs/_index/by-start/<run_id>` — the layout after the id moved to a
///   single key segment;
/// * `runs/_index/by-start/<YYYY-MM-DD>/<run_id>` — the older index encoded a
///   run id as a `created_at` date segment plus the id, so an operator could
///   scan a date range. Servers upgraded from that layout still hold these
///   markers, and refusing them aborts the whole activation.
///
/// Anything else stays unrecognized so a genuinely unexpected key still fails
/// closed instead of being silently ignored.
pub(crate) fn parse_run_catalog_key(raw: &str) -> Option<RunId> {
    let segments = SlateKey::segments(raw).collect::<Vec<_>>();
    match segments.as_slice() {
        ["runs", "_index", "by-start", run_id] => run_id.parse().ok(),
        ["runs", "_index", "by-start", date, run_id] => {
            if chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_err() {
                return None;
            }
            run_id.parse().ok()
        }
        _ => None,
    }
}

fn run_catalog_root() -> SlateKey {
    SlateKey::new("runs").with("_index").with("by-start")
}

// Sequence keys zero-pad `seq` to six digits so lexicographic key order
// matches numeric seq order through `MAX_EVENT_SEQ`. Seek-based event listing
// (`run_events_range`) depends on this invariant, so event allocation rejects
// larger sequences.
pub(crate) fn run_event_key(run_id: &RunId, seq: u32, epoch_ms: i64) -> SlateKey {
    SlateKey::new("runs")
        .with(run_id)
        .with("events")
        .with(format!("{seq:06}-{epoch_ms}"))
}

#[cfg(test)]
pub(crate) fn run_event_seq_prefix(run_id: &RunId, seq: u32) -> SlateKey {
    SlateKey::new("runs")
        .with(run_id)
        .with("events")
        .with(format!("{seq:06}-"))
}

/// Scan range covering the run's event keys from `start_seq` to the end of
/// the run's event namespace, so seek-based listing never touches keys of
/// other runs or namespaces.
#[cfg(test)]
pub(crate) fn run_events_range(run_id: &RunId, start_seq: u32) -> Range<SlateKey> {
    let end = SlateKey::new("runs")
        .with(run_id)
        .with("events")
        .into_prefix_end();
    run_event_seq_prefix(run_id, start_seq)..end
}

pub(crate) fn sessions_by_id_prefix() -> SlateKey {
    SlateKey::new("sessions").with("by-id").into_prefix()
}

#[cfg(test)]
pub(crate) fn session_by_id_key(session_id: &fabro_types::SessionId) -> SlateKey {
    SlateKey::new("sessions").with("by-id").with(session_id)
}

// --- Parsing ---

#[cfg(test)]
pub(crate) fn parse_event_seq(key: &str) -> Option<u32> {
    let mut segments = SlateKey::segments(key);
    let _ = segments.next()?; // "runs"
    let _ = segments.next()?; // run_id
    if segments.next()? != "events" {
        return None;
    }
    segments.next()?.split_once('-')?.0.parse().ok()
}

#[cfg(test)]
mod tests {
    use fabro_types::RunId;

    use super::*;

    #[test]
    fn builder_joins_segments_with_null_byte() {
        let key = SlateKey::new("a").with("b").with("c");
        assert_eq!(key.as_ref(), b"a\0b\0c");
    }

    #[test]
    fn into_prefix_appends_trailing_null_byte() {
        let key = SlateKey::new("a").with("b").into_prefix();
        assert_eq!(key.as_ref(), b"a\0b\0");
    }

    #[test]
    fn event_key_segments() {
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        let key = run_event_key(&run_id, 7, 123);
        let segments: Vec<&str> = SlateKey::segments(key.as_str()).collect();
        assert_eq!(segments, [
            "runs",
            "01JT56VE4Z5NZ814GZN2JZD65A",
            "events",
            "000007-123"
        ]);
    }

    #[test]
    fn sequence_keys_are_zero_padded() {
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        let key = run_event_key(&run_id, 7, 123);
        let leaf = SlateKey::segments(key.as_str()).last().unwrap();
        assert_eq!(leaf, "000007-123");
    }

    #[test]
    fn run_events_range_bounds_the_event_namespace() {
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        let range = run_events_range(&run_id, 2);
        let contains = |key: &SlateKey| {
            range.start.as_ref() <= key.as_ref() && key.as_ref() < range.end.as_ref()
        };

        assert!(!contains(&run_event_key(&run_id, 1, 123)));
        assert!(contains(&run_event_key(&run_id, 2, 123)));
        assert!(contains(&run_event_key(&run_id, MAX_EVENT_SEQ, 123)));
        // Sibling namespaces of the same run sort outside the range.
        assert!(!contains(&SlateKey::new("runs").with(run_id).with("state")));
        assert!(!contains(
            &session_by_id_key(&fabro_types::SessionId::new())
        ));
    }

    #[test]
    fn parse_run_catalog_key_accepts_both_retired_shapes() {
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        let canonical = SlateKey::new("runs")
            .with("_index")
            .with("by-start")
            .with(run_id);
        assert_eq!(parse_run_catalog_key(canonical.as_str()), Some(run_id));

        // The older index encoded a run id as `<created_at date>/<run_id>`;
        // servers upgraded from that layout still hold these markers.
        let dated = SlateKey::new("runs")
            .with("_index")
            .with("by-start")
            .with("2026-09-05")
            .with(run_id);
        assert_eq!(parse_run_catalog_key(dated.as_str()), Some(run_id));

        for rejected in [
            SlateKey::new("runs")
                .with("_index")
                .with("by-start")
                .with("2026-09-05"),
            SlateKey::new("runs")
                .with("_index")
                .with("by-start")
                .with("not-a-date")
                .with("01JT56VE4Z5NZ814GZN2JZD65A"),
            SlateKey::new("runs")
                .with("_index")
                .with("by-start")
                .with("2026-09-05")
                .with("01JT56VE4Z5NZ814GZN2JZD65A")
                .with("extra"),
            SlateKey::new("runs").with("_index").with("other").with("x"),
        ] {
            assert_eq!(
                parse_run_catalog_key(rejected.as_str()),
                None,
                "{rejected:?}"
            );
        }
    }

    #[test]
    fn parse_event_seq_roundtrips() {
        let run_id: RunId = "01JT56VE4Z5NZ814GZN2JZD65A".parse().unwrap();
        assert_eq!(
            parse_event_seq(run_event_key(&run_id, 7, 123).as_str()),
            Some(7)
        );
    }

    #[test]
    fn parse_event_seq_rejects_invalid_keys() {
        assert_eq!(
            parse_event_seq(
                SlateKey::new("runs")
                    .with("not-a-run")
                    .with("events")
                    .with("not-a-seq")
                    .as_str()
            ),
            None
        );
    }
}
