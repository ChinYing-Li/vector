use super::{Event, EventMetadata, LogEvent, Value};
use crate::config::log_schema;
use lookup::LookupBuf;

mod log;
mod metric;

use log::Target as LogTarget;
use metric::Target as MetricTarget;

/// An adapter to turn `Event`s into `vrl::Target`s.
#[derive(Debug, Clone)]
pub enum VrlTarget {
    Log(LogTarget),
    Metric(MetricTarget),
}

impl VrlTarget {
    pub fn new(event: Event) -> Self {
        match event {
            Event::Log(event) => VrlTarget::Log(LogTarget::Event(event)),
            Event::Metric(event) => VrlTarget::Metric(MetricTarget::Event(event)),
        }
    }

    /// Turn the target back into events.
    ///
    /// This returns an iterator of events as one event can be turned into multiple by assigning an
    /// array to `.` in VRL.
    pub fn into_events(self) -> impl Iterator<Item = Event> {
        match self {
            VrlTarget::Log(LogTarget::Event(log)) => {
                Box::new(std::iter::once(Event::Log(log))) as Box<dyn Iterator<Item = Event>>
            }
            VrlTarget::Log(LogTarget::Events(logs)) => {
                Box::new(logs.into_iter().map(Event::Log)) as Box<dyn Iterator<Item = Event>>
            }
            VrlTarget::Log(LogTarget::Value(value, metadata)) => {
                Box::new(value_into_events(value.into(), metadata))
                    as Box<dyn Iterator<Item = Event>>
            }
            VrlTarget::Metric(MetricTarget::Event(metric)) => {
                Box::new(std::iter::once(Event::Metric(metric))) as Box<dyn Iterator<Item = Event>>
            }
        }
    }
}

impl vrl::Target for VrlTarget {
    fn insert(&mut self, path: &LookupBuf, value: vrl::Value) -> Result<(), String> {
        match self {
            VrlTarget::Log(log) => log.insert(path, value),
            VrlTarget::Metric(metric) => metric.insert(path, value),
        }
    }

    fn get(&self, path: &LookupBuf) -> Result<Option<vrl::Value>, String> {
        match self {
            VrlTarget::Log(log) => log.get(path),
            VrlTarget::Metric(metric) => metric.get(path),
        }
    }

    fn remove(&mut self, path: &LookupBuf, compact: bool) -> Result<Option<vrl::Value>, String> {
        match self {
            VrlTarget::Log(log) => log.remove(path, compact),
            VrlTarget::Metric(metric) => metric.remove(path, compact),
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
