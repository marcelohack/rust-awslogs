//! Time-string parsing that mirrors the Python implementation.
//!
//! Accepts either:
//!   * a relative form: `2m`, `2 minutes`, `2 minutes ago`, `2h`, `2d`, `2w`, ...
//!   * an absolute form parsed by a tolerant date parser
//!
//! Always returns epoch milliseconds in UTC.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};
use regex::Regex;
use std::sync::OnceLock;

use crate::exceptions::AwsLogsError;

/// Source of "now" — overridable in tests.
pub trait Clock: Send + Sync {
    fn utcnow(&self) -> DateTime<Utc>;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn utcnow(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

fn ago_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // Matches Python's `re.match` (anchored at start only, trailing text ignored).
        Regex::new(r"^(\d+)\s?(m|minute|minutes|h|hour|hours|d|day|days|w|weeks|weeks)(?: ago)?")
            .unwrap()
    })
}

/// Parse `s` (a "5m", "2h ago", "1/1/2015 12:34", "2016-08-31T02:23:25.000Z" ...) into epoch ms.
///
/// Returns `Ok(None)` if `s` is empty.
pub fn parse_datetime(s: Option<&str>) -> Result<Option<i64>, AwsLogsError> {
    parse_datetime_with(s, &SystemClock)
}

pub fn parse_datetime_with(
    s: Option<&str>,
    clock: &dyn Clock,
) -> Result<Option<i64>, AwsLogsError> {
    let Some(raw) = s else { return Ok(None) };
    if raw.is_empty() {
        return Ok(None);
    }

    if let Some(caps) = ago_re().captures(raw) {
        let amount: i64 = caps[1].parse().expect("digit regex");
        let unit = caps[2].chars().next().unwrap();
        let secs: i64 = match unit {
            'm' => 60,
            'h' => 3600,
            'd' => 86400,
            'w' => 604800,
            _ => unreachable!(),
        };
        let now = clock.utcnow();
        let dt = now - chrono::Duration::seconds(secs * amount);
        return Ok(Some(dt.timestamp_millis() / 1000 * 1000));
    }

    // Absolute date parsing — try a series of formats covering the cases the
    // Python `dateutil.parser.parse` test suite exercises.
    if let Some(dt) = try_parse_absolute(raw) {
        let utc = if dt.timezone().fix().local_minus_utc() == 0 {
            dt.naive_utc()
        } else {
            dt.with_timezone(&Utc).naive_utc()
        };
        let epoch_secs = utc.and_utc().timestamp();
        return Ok(Some(epoch_secs * 1000));
    }

    Err(AwsLogsError::UnknownDate(raw.to_string()))
}

/// Try a curated set of absolute date/time formats. Returns a `DateTime` whose
/// timezone is either UTC or a fixed offset. Naive inputs are treated as UTC,
/// matching the Python behaviour of `dateutil.parser.parse` on an unzoned string
/// followed by `replace(tzinfo=None)`.
fn try_parse_absolute(raw: &str) -> Option<DateTime<chrono::FixedOffset>> {
    use chrono::FixedOffset;

    // 1. RFC3339 / ISO with tz
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(raw) {
        return Some(dt);
    }

    // 2. "YYYY-MM-DD HH:MM:SS UTC[+-]N"
    if let Some(dt) = parse_with_utc_offset(raw) {
        return Some(dt);
    }

    // 3. Naive formats — promote to UTC.
    let utc_zero = FixedOffset::east_opt(0).unwrap();
    let naive_formats = [
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ];
    for fmt in naive_formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(raw, fmt) {
            return Some(utc_zero.from_utc_datetime(&ndt));
        }
        if let Ok(d) = NaiveDate::parse_from_str(raw, fmt) {
            let ndt = d.and_time(NaiveTime::MIN);
            return Some(utc_zero.from_utc_datetime(&ndt));
        }
    }

    // 4. dateutil-style D/M/YYYY [HH:MM[:SS]] — common Python test cases.
    if let Some(ndt) = parse_dmy(raw) {
        return Some(utc_zero.from_utc_datetime(&ndt));
    }

    None
}

fn parse_with_utc_offset(raw: &str) -> Option<DateTime<chrono::FixedOffset>> {
    // Form: "2016-08-31 10:23:25 UTC-8" or "UTC+05".
    let re = Regex::new(r"^(.*?)\s+UTC([+-]\d{1,2})(?::?(\d{2}))?$").ok()?;
    let caps = re.captures(raw)?;
    let stem = caps.get(1)?.as_str();
    let hours: i32 = caps.get(2)?.as_str().parse().ok()?;
    let minutes: i32 = caps.get(3).map_or(0, |m| m.as_str().parse().unwrap_or(0));
    let sign = if hours >= 0 { 1 } else { -1 };
    let secs = sign * (hours.abs() * 3600 + minutes * 60);
    let offset = chrono::FixedOffset::east_opt(secs)?;

    let candidate_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"];
    for fmt in candidate_formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(stem, fmt) {
            return offset.from_local_datetime(&ndt).single();
        }
    }
    None
}

fn parse_dmy(raw: &str) -> Option<NaiveDateTime> {
    // "D/M/YYYY", optional " H:M" or " H:M:S"
    let re =
        Regex::new(r"^(\d{1,2})/(\d{1,2})/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$").ok()?;
    let caps = re.captures(raw)?;
    let day: u32 = caps.get(1)?.as_str().parse().ok()?;
    let month: u32 = caps.get(2)?.as_str().parse().ok()?;
    let year: i32 = caps.get(3)?.as_str().parse().ok()?;
    let hour: u32 = caps.get(4).map_or(Ok(0), |m| m.as_str().parse()).ok()?;
    let minute: u32 = caps.get(5).map_or(Ok(0), |m| m.as_str().parse()).ok()?;
    let second: u32 = caps.get(6).map_or(Ok(0), |m| m.as_str().parse()).ok()?;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let time = NaiveTime::from_hms_opt(hour, minute, second)?;
    Some(date.and_time(time)).filter(|dt| dt.year() == year)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    struct FixedClock(DateTime<Utc>);
    impl Clock for FixedClock {
        fn utcnow(&self) -> DateTime<Utc> {
            self.0
        }
    }

    fn iso2epoch(s: &str) -> i64 {
        let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap();
        ndt.and_utc().timestamp() * 1000
    }

    #[test]
    fn parses_relative_and_absolute_dates() {
        let clock = FixedClock(Utc.with_ymd_and_hms(2015, 1, 1, 3, 0, 0).unwrap());

        assert_eq!(parse_datetime_with(Some(""), &clock).unwrap(), None);
        assert_eq!(parse_datetime_with(None, &clock).unwrap(), None);

        let plan: &[(&str, &str)] = &[
            ("2015-01-01 02:59:00", "1m"),
            ("2015-01-01 02:59:00", "1m ago"),
            ("2015-01-01 02:59:00", "1minute"),
            ("2015-01-01 02:59:00", "1minute ago"),
            ("2015-01-01 02:59:00", "1minutes"),
            ("2015-01-01 02:59:00", "1minutes ago"),
            ("2015-01-01 02:00:00", "1h"),
            ("2015-01-01 02:00:00", "1h ago"),
            ("2015-01-01 02:00:00", "1hour"),
            ("2015-01-01 02:00:00", "1hour ago"),
            ("2015-01-01 02:00:00", "1hours"),
            ("2015-01-01 02:00:00", "1hours ago"),
            ("2014-12-31 03:00:00", "1d"),
            ("2014-12-31 03:00:00", "1d ago"),
            ("2014-12-31 03:00:00", "1day"),
            ("2014-12-31 03:00:00", "1day ago"),
            ("2014-12-31 03:00:00", "1days"),
            ("2014-12-31 03:00:00", "1days ago"),
            ("2014-12-25 03:00:00", "1w"),
            ("2014-12-25 03:00:00", "1w ago"),
            ("2014-12-25 03:00:00", "1week"),
            ("2014-12-25 03:00:00", "1week ago"),
            ("2014-12-25 03:00:00", "1weeks"),
            ("2014-12-25 03:00:00", "1weeks ago"),
            ("2013-01-01 00:00:00", "1/1/2013"),
            ("2012-01-01 12:34:00", "1/1/2012 12:34"),
            ("2011-01-01 12:34:56", "1/1/2011 12:34:56"),
            ("2016-08-31 02:23:25", "2016-08-31T02:23:25.000Z"),
            ("2016-08-31 18:23:25", "2016-08-31 10:23:25 UTC-8"),
        ];
        for (expected, input) in plan {
            assert_eq!(
                parse_datetime_with(Some(input), &clock).unwrap(),
                Some(iso2epoch(expected)),
                "input={input}"
            );
        }

        assert!(matches!(
            parse_datetime_with(Some("???"), &clock),
            Err(AwsLogsError::UnknownDate(_))
        ));
    }
}
