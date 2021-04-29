use super::{Event, EventMetadata, LogEvent, Metric, MetricKind, Value};
use crate::config::log_schema;
use snafu::Snafu;
use std::{collections::BTreeMap, convert::TryFrom, iter::FromIterator};
use vrl::path::Segment;

const VALID_METRIC_PATHS_SET: &str = ".name, .namespace, .timestamp, .kind, .tags";

/// We can get the `type` of the metric in Remap, but can't set  it.
const VALID_METRIC_PATHS_GET: &str = ".name, .namespace, .timestamp, .kind, .tags, .type";

/// An adapter to turn `Event`s into `vr::Target`s
#[derive(Debug, Clone)]
pub enum VrlTarget {
    LogEvent(vrl::Value, EventMetadata),
    Metric(Metric),
}

impl VrlTarget {
    pub fn new(event: Event) -> Self {
        match event {
            Event::Log(event) => {
                let metadata = event.metadata().to_owned();
                let fields: BTreeMap<String, Value> = event.into();
                let value: Value = fields.into();
                VrlTarget::LogEvent(value.into(), metadata)
            }
            Event::Metric(event) => VrlTarget::Metric(event),
        }
    }

    /// Turn the target back into events
    ///
    /// This returns an iterator of events as one event can be turned into multiple by assign `.`
    /// to an array in VRL
    pub fn into_events(self) -> impl Iterator<Item = Event> {
        match self {
            VrlTarget::LogEvent(value, metadata) => {
                Box::new(value_into_events(value.into(), metadata))
                    as Box<dyn Iterator<Item = Event>>
            }
            VrlTarget::Metric(metric) => {
                Box::new(std::iter::once(Event::Metric(metric))) as Box<dyn Iterator<Item = Event>>
            }
        }
    }
}

impl vrl::Target for VrlTarget {
    fn insert(&mut self, path: &vrl::Path, value: vrl::Value) -> std::result::Result<(), String> {
        match self {
            VrlTarget::LogEvent(ref mut log, _) => log.insert(path, value),
            VrlTarget::Metric(ref mut metric) => {
                if path.is_root() {
                    return Err(MetricPathError::SetPathError.to_string());
                }

                match path.segments() {
                    [Segment::Field(tags)] if tags.as_str() == "tags" => {
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
                        Ok(())
                    }
                    [Segment::Field(tags), Segment::Field(field)] if tags.as_str() == "tags" => {
                        let value = value.try_bytes().map_err(|e| e.to_string())?;
                        metric.set_tag_value(
                            field.as_str().to_owned(),
                            String::from_utf8_lossy(&value).into_owned(),
                        );
                        Ok(())
                    }
                    [Segment::Field(name)] if name.as_str() == "name" => {
                        let value = value.try_bytes().map_err(|e| e.to_string())?;
                        metric.series.name.name = String::from_utf8_lossy(&value).into_owned();
                        Ok(())
                    }
                    [Segment::Field(namespace)] if namespace.as_str() == "namespace" => {
                        let value = value.try_bytes().map_err(|e| e.to_string())?;
                        metric.series.name.namespace =
                            Some(String::from_utf8_lossy(&value).into_owned());
                        Ok(())
                    }
                    [Segment::Field(timestamp)] if timestamp.as_str() == "timestamp" => {
                        let value = value.try_timestamp().map_err(|e| e.to_string())?;
                        metric.data.timestamp = Some(value);
                        Ok(())
                    }
                    [Segment::Field(kind)] if kind.as_str() == "kind" => {
                        metric.data.kind = MetricKind::try_from(value)?;
                        Ok(())
                    }
                    _ => Err(MetricPathError::InvalidPath {
                        path: &path.to_string(),
                        expected: VALID_METRIC_PATHS_SET,
                    }
                    .to_string()),
                }
            }
        }
    }

    fn get(&self, path: &vrl::Path) -> std::result::Result<Option<vrl::Value>, String> {
        match self {
            VrlTarget::LogEvent(log, _) => log.get(path),
            VrlTarget::Metric(metric) => {
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

                match path.segments() {
                    [Segment::Field(name)] if name.as_str() == "name" => {
                        Ok(Some(metric.name().to_string().into()))
                    }
                    [Segment::Field(namespace)] if namespace.as_str() == "namespace" => {
                        Ok(metric.series.name.namespace.clone().map(Into::into))
                    }
                    [Segment::Field(timestamp)] if timestamp.as_str() == "timestamp" => {
                        Ok(metric.data.timestamp.map(Into::into))
                    }
                    [Segment::Field(kind)] if kind.as_str() == "kind" => {
                        Ok(Some(metric.data.kind.clone().into()))
                    }
                    [Segment::Field(tags)] if tags.as_str() == "tags" => {
                        Ok(metric.tags().map(|map| {
                            let iter = map.iter().map(|(k, v)| (k.to_owned(), v.to_owned().into()));
                            vrl::Value::from_iter(iter)
                        }))
                    }
                    [Segment::Field(tags), Segment::Field(field)] if tags.as_str() == "tags" => {
                        Ok(metric.tag_value(field.as_str()).map(|value| value.into()))
                    }
                    [Segment::Field(type_)] if type_.as_str() == "type" => {
                        Ok(Some(metric.data.value.clone().into()))
                    }
                    _ => Err(MetricPathError::InvalidPath {
                        path: &path.to_string(),
                        expected: VALID_METRIC_PATHS_GET,
                    }
                    .to_string()),
                }
            }
        }
    }

    fn remove(
        &mut self,
        path: &vrl::Path,
        compact: bool,
    ) -> std::result::Result<Option<vrl::Value>, String> {
        match self {
            VrlTarget::LogEvent(ref mut log, _) => log.remove(path, compact),
            VrlTarget::Metric(ref mut metric) => {
                if path.is_root() {
                    return Err(MetricPathError::SetPathError.to_string());
                }

                match path.segments() {
                    [Segment::Field(namespace)] if namespace.as_str() == "namespace" => {
                        Ok(metric.series.name.namespace.take().map(Into::into))
                    }
                    [Segment::Field(timestamp)] if timestamp.as_str() == "timestamp" => {
                        Ok(metric.data.timestamp.take().map(Into::into))
                    }
                    [Segment::Field(tags)] if tags.as_str() == "tags" => {
                        Ok(metric.series.tags.take().map(|map| {
                            let iter = map.into_iter().map(|(k, v)| (k, v.into()));
                            vrl::Value::from_iter(iter)
                        }))
                    }
                    [Segment::Field(tags), Segment::Field(field)] if tags.as_str() == "tags" => {
                        Ok(metric.delete_tag(field.as_str()).map(Into::into))
                    }
                    _ => Err(MetricPathError::InvalidPath {
                        path: &path.to_string(),
                        expected: VALID_METRIC_PATHS_SET,
                    }
                    .to_string()),
                }
            }
        }
    }
}

impl From<Event> for VrlTarget {
    fn from(event: Event) -> Self {
        VrlTarget::new(event)
    }
}

fn value_into_events(value: Value, metadata: EventMetadata) -> impl Iterator<Item = Event> {
    match value {
        Value::Array(values) => Box::new(values.into_iter().map(move |v| {
            let mut log = LogEvent::new_with_metadata(metadata.clone());
            log.insert(log_schema().message_key(), v);
            Event::from(log)
        })) as Box<dyn Iterator<Item = Event>>,
        Value::Map(object) => {
            let mut log = LogEvent::new_with_metadata(metadata);
            log.extend(object);
            Box::new(std::iter::once(Event::from(log))) as Box<dyn Iterator<Item = Event>>
        }
        v => {
            let mut log = LogEvent::new_with_metadata(metadata);
            log.insert(log_schema().message_key(), v);
            Box::new(std::iter::once(Event::from(log))) as Box<dyn Iterator<Item = Event>>
        }
    }
}

#[derive(Debug, Snafu)]
enum MetricPathError<'a> {
    #[snafu(display("cannot set root path"))]
    SetPathError,

    #[snafu(display("invalid path {}: expected one of {}", path, expected))]
    InvalidPath { path: &'a str, expected: &'a str },
}

#[cfg(test)]
mod test {
    use super::super::{metric::MetricTags, MetricValue};
    use super::*;
    use chrono::{offset::TimeZone, Utc};
    use pretty_assertions::assert_eq;
    use shared::btreemap;
    use std::str::FromStr;
    use vrl::{Path, Target, Value};

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

        let target = VrlTarget::new(Event::Metric(metric));

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
            target.get(&Path::from_str(".").unwrap())
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

        let mut target = VrlTarget::new(Event::Metric(metric));

        for (path, current, new, delete) in cases {
            let path = Path::from_str(path).unwrap();

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

        let mut target = VrlTarget::new(Event::Metric(metric));

        assert_eq!(
            Err(format!(
                "invalid path .zork: expected one of {}",
                validpaths_get.join(", ")
            )),
            target.get(&Path::from_str("zork").unwrap())
        );

        assert_eq!(
            Err(format!(
                "invalid path .zork: expected one of {}",
                validpaths_set.join(", ")
            )),
            target.insert(&Path::from_str("zork").unwrap(), "thing".into())
        );

        assert_eq!(
            Err(format!(
                "invalid path .zork: expected one of {}",
                validpaths_set.join(", ")
            )),
            target.remove(&Path::from_str("zork").unwrap(), true)
        );

        assert_eq!(
            Err(format!(
                "invalid path .tags.foo.flork: expected one of {}",
                validpaths_get.join(", ")
            )),
            target.get(&Path::from_str("tags.foo.flork").unwrap())
        );
    }
}
