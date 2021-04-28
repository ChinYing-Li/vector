use super::{Event, Metric, MetricKind, Value};
use snafu::Snafu;
use std::{collections::BTreeMap, convert::TryFrom, iter::FromIterator};
use vrl::path::Segment;

const VALID_METRIC_PATHS_SET: &str = ".name, .namespace, .timestamp, .kind, .tags";

/// We can get the `type` of the metric in Remap, but can't set  it.
const VALID_METRIC_PATHS_GET: &str = ".name, .namespace, .timestamp, .kind, .tags, .type";

#[derive(Debug, Clone)]
pub enum VrlTarget {
    LogEvent(vrl::Value),
    Metric(Metric),
}

impl VrlTarget {
    pub fn into_events(self) -> impl Iterator<Item = Event> {
        match self {
            VrlTarget::LogEvent(value) => {
                Box::new(value_into_events(value)) as Box<dyn Iterator<Item = Event>>
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
            VrlTarget::LogEvent(ref mut log) => log.insert(path, value),
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
            VrlTarget::LogEvent(log) => log.get(path),
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
            VrlTarget::LogEvent(ref mut log) => log.remove(path, compact),
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
        match event {
            // TODO optimize
            Event::Log(mut event) => VrlTarget::LogEvent(vrl::Value::Object(
                event
                    .take_fields()
                    .into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect(),
            )),
            Event::Metric(event) => VrlTarget::Metric(event),
        }
    }
}

fn value_into_events(value: vrl::Value) -> impl Iterator<Item = Event> {
    match value {
        vrl::Value::Array(values) => Box::new(values.into_iter().map(value_into_events).flatten())
            as Box<dyn Iterator<Item = Event>>,
        vrl::Value::Object(object) => {
            // TODO optimize
            let event = object
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect::<BTreeMap<String, Value>>()
                .into();

            Box::new(std::iter::once(event)) as Box<dyn Iterator<Item = Event>>
        }
        vrl::Value::Bytes(bytes) => {
            Box::new(std::iter::once(Event::from(bytes))) as Box<dyn Iterator<Item = Event>>
        }
        v => {
            Box::new(std::iter::once(Event::from(v.to_string()))) as Box<dyn Iterator<Item = Event>>
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
    // TODO put metric tests back here
}
