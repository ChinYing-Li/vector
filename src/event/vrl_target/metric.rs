use super::super::{Metric, MetricKind};
use lookup::LookupBuf;
use snafu::Snafu;
use std::{collections::BTreeMap, convert::TryFrom, iter::FromIterator};

const VALID_METRIC_PATHS_SET: &str = ".name, .namespace, .timestamp, .kind, .tags";

/// We can get the `type` of the metric in Remap, but can't set it.
const VALID_METRIC_PATHS_GET: &str = ".name, .namespace, .timestamp, .kind, .tags, .type";

/// Metrics aren't interested in paths that have a length longer than 3
/// The longest path is 2, and we need to check that a third segment doesn't exist as we don't want
/// fields such as `.tags.host.thing`.
const MAX_METRIC_PATH_DEPTH: usize = 3;

#[derive(Debug, Snafu)]
enum MetricPathError<'a> {
    #[snafu(display("cannot set root path"))]
    SetPathError,

    #[snafu(display("invalid path {}: expected one of {}", path, expected))]
    InvalidPath { path: &'a str, expected: &'a str },
}

#[derive(Debug, Clone)]
pub enum Target {
    Event(Metric),
}

impl vrl::Target for Target {
    fn insert(&mut self, path: &LookupBuf, value: vrl::Value) -> Result<(), String> {
        match self {
            Target::Event(ref mut metric) => {
                if path.is_root() {
                    return Err(MetricPathError::SetPathError.to_string());
                }

                if let Some(paths) = path.to_alternative_components(MAX_METRIC_PATH_DEPTH).get(0) {
                    match paths.as_slice() {
                        ["tags"] => {
                            let value = value.try_object().map_err(|e| e.to_string())?;
                            for (field, value) in value.iter() {
                                metric.set_tag_value(
                                    field.as_str().to_owned(),
                                    value
                                        .try_bytes_utf8_lossy()
                                        .map_err(|e| e.to_string())?
                                        .into_owned(),
                                );
                            }
                            return Ok(());
                        }
                        ["tags", field] => {
                            let value = value.try_bytes().map_err(|e| e.to_string())?;
                            metric.set_tag_value(
                                field.to_string(),
                                String::from_utf8_lossy(&value).into_owned(),
                            );
                            return Ok(());
                        }
                        ["name"] => {
                            let value = value.try_bytes().map_err(|e| e.to_string())?;
                            metric.series.name.name = String::from_utf8_lossy(&value).into_owned();
                            return Ok(());
                        }
                        ["namespace"] => {
                            let value = value.try_bytes().map_err(|e| e.to_string())?;
                            metric.series.name.namespace =
                                Some(String::from_utf8_lossy(&value).into_owned());
                            return Ok(());
                        }
                        ["timestamp"] => {
                            let value = value.try_timestamp().map_err(|e| e.to_string())?;
                            metric.data.timestamp = Some(value);
                            return Ok(());
                        }
                        ["kind"] => {
                            metric.data.kind = MetricKind::try_from(value)?;
                            return Ok(());
                        }
                        _ => {
                            return Err(MetricPathError::InvalidPath {
                                path: &path.to_string(),
                                expected: VALID_METRIC_PATHS_SET,
                            }
                            .to_string())
                        }
                    }
                }

                Err(MetricPathError::InvalidPath {
                    path: &path.to_string(),
                    expected: VALID_METRIC_PATHS_SET,
                }
                .to_string())
            }
        }
    }

    fn get(&self, path: &LookupBuf) -> Result<Option<vrl::Value>, String> {
        match self {
            Target::Event(metric) => {
                if path.is_root() {
                    let mut map = BTreeMap::<String, vrl::Value>::new();
                    map.insert("name".to_string(), metric.series.name.name.clone().into());
                    if let Some(ref namespace) = metric.series.name.namespace {
                        map.insert("namespace".to_string(), namespace.clone().into());
                    }
                    if let Some(timestamp) = metric.data.timestamp {
                        map.insert("timestamp".to_string(), timestamp.into());
                    }
                    map.insert("kind".to_string(), metric.data.kind.into());
                    if let Some(tags) = metric.tags() {
                        map.insert(
                            "tags".to_string(),
                            tags.iter()
                                .map(|(tag, value)| (tag.clone(), value.clone().into()))
                                .collect::<BTreeMap<_, _>>()
                                .into(),
                        );
                    }
                    map.insert("type".to_string(), metric.data.value.clone().into());

                    return Ok(Some(map.into()));
                }

                for paths in path.to_alternative_components(MAX_METRIC_PATH_DEPTH) {
                    match paths.as_slice() {
                        ["name"] => return Ok(Some(metric.name().to_string().into())),
                        ["namespace"] => match &metric.series.name.namespace {
                            Some(namespace) => return Ok(Some(namespace.clone().into())),
                            None => continue,
                        },
                        ["timestamp"] => match metric.data.timestamp {
                            Some(timestamp) => return Ok(Some(timestamp.into())),
                            None => continue,
                        },
                        ["kind"] => return Ok(Some(metric.data.kind.clone().into())),
                        ["tags"] => {
                            return Ok(metric.tags().map(|map| {
                                let iter =
                                    map.iter().map(|(k, v)| (k.to_owned(), v.to_owned().into()));
                                vrl::Value::from_iter(iter)
                            }))
                        }
                        ["tags", field] => match metric.tag_value(field) {
                            Some(value) => return Ok(Some(value.into())),
                            None => continue,
                        },
                        ["type"] => return Ok(Some(metric.data.value.clone().into())),
                        _ => {
                            return Err(MetricPathError::InvalidPath {
                                path: &path.to_string(),
                                expected: VALID_METRIC_PATHS_GET,
                            }
                            .to_string())
                        }
                    }
                }

                // We only reach this point if we have requested a tag that doesn't exist or an empty
                // field.
                Ok(None)
            }
        }
    }

    fn remove(&mut self, path: &LookupBuf, _compact: bool) -> Result<Option<vrl::Value>, String> {
        match self {
            Target::Event(ref mut metric) => {
                if path.is_root() {
                    return Err(MetricPathError::SetPathError.to_string());
                }

                if let Some(paths) = path.to_alternative_components(MAX_METRIC_PATH_DEPTH).get(0) {
                    match paths.as_slice() {
                        ["namespace"] => {
                            return Ok(metric.series.name.namespace.take().map(Into::into))
                        }
                        ["timestamp"] => return Ok(metric.data.timestamp.take().map(Into::into)),
                        ["tags"] => {
                            return Ok(metric.series.tags.take().map(|map| {
                                let iter = map.into_iter().map(|(k, v)| (k, v.into()));
                                vrl::Value::from_iter(iter)
                            }))
                        }
                        ["tags", field] => return Ok(metric.delete_tag(field).map(Into::into)),
                        _ => {
                            return Err(MetricPathError::InvalidPath {
                                path: &path.to_string(),
                                expected: VALID_METRIC_PATHS_SET,
                            }
                            .to_string())
                        }
                    }
                }

                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::super::{metric::MetricTags, MetricValue};
    use super::*;
    use chrono::{offset::TimeZone, Utc};
    use pretty_assertions::assert_eq;
    use shared::btreemap;
    use vrl::{Target as _, Value};

    #[test]
    fn metric_all_fields() {
        let metric = Metric::new(
            "zub",
            MetricKind::Absolute,
            MetricValue::Counter { value: 1.23 },
        )
        .with_namespace(Some("zoob"))
        .with_tags(Some({
            let mut map = MetricTags::new();
            map.insert("tig".to_string(), "tog".to_string());
            map
        }))
        .with_timestamp(Some(Utc.ymd(2020, 12, 10).and_hms(12, 0, 0)));

        let target = Target::Event(metric);

        assert_eq!(
            Ok(Some(
                btreemap! {
                    "name" => "zub",
                    "namespace" => "zoob",
                    "timestamp" => Utc.ymd(2020, 12, 10).and_hms(12, 0, 0),
                    "tags" => btreemap! { "tig" => "tog" },
                    "kind" => "absolute",
                    "type" => "counter",
                }
                .into()
            )),
            target.get(&LookupBuf::root())
        );
    }

    #[test]
    fn metric_fields() {
        let metric = Metric::new(
            "name",
            MetricKind::Absolute,
            MetricValue::Counter { value: 1.23 },
        )
        .with_tags(Some({
            let mut map = MetricTags::new();
            map.insert("tig".to_string(), "tog".to_string());
            map
        }));

        let cases = vec![
            (
                "name",                    // Path
                Some(Value::from("name")), // Current value
                Value::from("namefoo"),    // New value
                false,                     // Test deletion
            ),
            ("namespace", None, "namespacefoo".into(), true),
            (
                "timestamp",
                None,
                Utc.ymd(2020, 12, 8).and_hms(12, 0, 0).into(),
                true,
            ),
            (
                "kind",
                Some(Value::from("absolute")),
                "incremental".into(),
                false,
            ),
            ("tags.thing", None, "footag".into(), true),
        ];

        let mut target = Target::Event(metric);

        for (path, current, new, delete) in cases {
            let path = LookupBuf::from_str(path).unwrap();

            assert_eq!(Ok(current), target.get(&path));
            assert_eq!(Ok(()), target.insert(&path, new.clone()));
            assert_eq!(Ok(Some(new.clone())), target.get(&path));

            if delete {
                assert_eq!(Ok(Some(new)), target.remove(&path, true));
                assert_eq!(Ok(None), target.get(&path));
            }
        }
    }

    #[test]
    fn metric_invalid_paths() {
        let metric = Metric::new(
            "name",
            MetricKind::Absolute,
            MetricValue::Counter { value: 1.23 },
        );

        let validpaths_get = vec![
            ".name",
            ".namespace",
            ".timestamp",
            ".kind",
            ".tags",
            ".type",
        ];

        let validpaths_set = vec![".name", ".namespace", ".timestamp", ".kind", ".tags"];

        let mut target = Target::Event(metric);

        assert_eq!(
            Err(format!(
                "invalid path zork: expected one of {}",
                validpaths_get.join(", ")
            )),
            target.get(&LookupBuf::from_str("zork").unwrap())
        );

        assert_eq!(
            Err(format!(
                "invalid path zork: expected one of {}",
                validpaths_set.join(", ")
            )),
            target.insert(&LookupBuf::from_str("zork").unwrap(), "thing".into())
        );

        assert_eq!(
            Err(format!(
                "invalid path zork: expected one of {}",
                validpaths_set.join(", ")
            )),
            target.remove(&LookupBuf::from_str("zork").unwrap(), true)
        );

        assert_eq!(
            Err(format!(
                "invalid path tags.foo.flork: expected one of {}",
                validpaths_get.join(", ")
            )),
            target.get(&LookupBuf::from_str("tags.foo.flork").unwrap())
        );
    }
}
