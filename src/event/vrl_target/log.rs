use super::super::{EventMetadata, LogEvent, Value};
use lookup::LookupBuf;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum Target {
    Event(LogEvent),
    Events(Vec<LogEvent>),
    Value(vrl::Value, EventMetadata),
}

impl vrl::Target for Target {
    fn insert(&mut self, path: &LookupBuf, value: vrl::Value) -> Result<(), String> {
        match self {
            Target::Value(ref mut log, _) => log.insert(path, value),
            Target::Event(ref mut log) => {
                let mut value = Value::from(value);
                if path.is_root() {
                    if let Value::Map(map) = value {
                        // TODO metadata
                        *log = LogEvent::from(map);
                        Ok(())
                    } else {
                        Err("Cannot insert as root of Event unless it is a map.".into())
                    }
                } else {
                    let _val = log.insert_path(path.into(), value);
                    Ok(())
                }
            }
            _ => Ok(()), // TODO
        }
    }

    fn get(&self, path: &LookupBuf) -> Result<Option<vrl::Value>, String> {
        match self {
            Target::Value(value, _) => value.get(path),
            Target::Event(log) => {
                if path.is_root() {
                    let fields: BTreeMap<String, Value> = log.into();
                    Ok(Some(fields.clone().into()))
                } else {
                    let val = log.get(path);
                    Ok(val.map(|val| val.clone().into()))
                }
            }
            _ => Ok(None), // TODO
        }
    }

    fn remove(&mut self, path: &LookupBuf, compact: bool) -> Result<Option<vrl::Value>, String> {
        match self {
            Target::Value(ref mut value, _) => value.remove(path, compact),
            Target::Event(ref mut log) => {
                if path.is_root() {
                    Ok(Some({
                        let mut map = BTreeMap::new();
                        std::mem::swap(log.as_map_mut(), &mut map);
                        map.into_iter()
                            .map(|(key, value)| (key, value.into()))
                            .collect::<BTreeMap<_, _>>()
                            .into()
                    }))
                } else {
                    // TODO handle compact
                    let val = log.remove(path);
                    val.map(|val| val.map(|val| val.into()))
                        .map_err(|err| err.to_string())
                }
            }
            _ => Ok(None), // TODO
        }
    }
}
