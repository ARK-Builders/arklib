mod atomic;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    io::{Read, Result, Write},
    usize,
};

pub use atomic::AtomicFile;
pub use merging::merge_values;

pub fn modify(
    atomic_file: &AtomicFile,
    mut operator: impl FnMut(&[u8]) -> Vec<u8>,
) -> Result<usize> {
    let mut buf = vec![];
    loop {
        let latest = atomic_file.load()?;
        buf.clear();
        if let Some(mut file) = latest.open()? {
            file.read_to_end(&mut buf)?;
        }
        let data = operator(&buf);
        let tmp = atomic_file.make_temp()?;
        (&tmp).write_all(&data)?;
        (&tmp).flush()?;
        match atomic_file.compare_and_swap(&latest, tmp) {
            Ok(val) => return Ok(val),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                continue
            }
            Err(err) => return Err(err),
        }
    }
}

pub fn modify_json<T: Serialize + DeserializeOwned>(
    atomic_file: &AtomicFile,
    mut operator: impl FnMut(&mut Option<T>),
) -> std::io::Result<usize> {
    loop {
        let latest = atomic_file.load()?;
        let mut val = None;
        if let Some(file) = latest.open()? {
            val = Some(serde_json::from_reader(std::io::BufReader::new(file))?);
        }
        operator(&mut val);
        let tmp = atomic_file.make_temp()?;
        let mut w = std::io::BufWriter::new(&tmp);
        serde_json::to_writer(&mut w, &val)?;
        w.flush()?;
        drop(w);
        match atomic_file.compare_and_swap(&latest, tmp) {
            Ok(val) => return Ok(val),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                continue
            }
            Err(err) => return Err(err),
        }
    }
}

mod merging {
    use serde_json::json;
    use serde_json::map::Entry;
    use serde_json::Map;
    use serde_json::Value;

    pub fn merge_values(origin: Value, new_data: Value) -> Value {
        match (origin, new_data) {
            (Value::Object(old), Value::Object(new)) => merge_object(old, new),
            (Value::Array(old), Value::Array(new)) => merge_vec(old, new),
            (Value::Array(mut old), new) => {
                if !old.is_empty()
                    && std::mem::discriminant(&old[0])
                        == std::mem::discriminant(&new)
                {
                    old.push(new);
                    Value::Array(old)
                } else if old.is_empty() {
                    json!([new])
                } else {
                    Value::Array(old)
                }
            }
            (old, Value::Array(mut new_data)) => {
                if !new_data.is_empty()
                    && std::mem::discriminant(&old)
                        == std::mem::discriminant(&new_data[0])
                {
                    new_data.insert(0, old);
                    Value::Array(new_data)
                } else {
                    // Different types, keep old data
                    old
                }
            }
            (old, Value::Null) => old,
            (Value::Null, new) => new,
            (old, new) => {
                if std::mem::discriminant(&old) == std::mem::discriminant(&new)
                    && old != new
                {
                    json!([old, new])
                } else {
                    // different types keep old data
                    old
                }
            }
        }
    }

    fn merge_object(
        mut origin: Map<String, Value>,
        new_data: Map<String, Value>,
    ) -> Value {
        for (key, value) in new_data.into_iter() {
            match origin.entry(&key) {
                Entry::Vacant(e) => {
                    e.insert(value);
                }
                Entry::Occupied(prev) => {
                    // Extract entry to manipulate it
                    let prev = prev.remove();
                    match (prev, value) {
                        (Value::Array(old_data), Value::Array(new_data)) => {
                            let updated = merge_vec(old_data, new_data);
                            origin.insert(key, updated);
                        }
                        (Value::Array(d), Value::Null) => {
                            origin.insert(key, Value::Array(d));
                        }
                        (Value::Array(mut old_data), new_data) => {
                            if old_data.iter().all(|val| {
                                std::mem::discriminant(&new_data)
                                    == std::mem::discriminant(val)
                            }) {
                                old_data.push(new_data);
                            }
                            origin.insert(key, json!(old_data));
                        }
                        (old, Value::Array(mut new_data)) => {
                            if new_data.iter().all(|val| {
                                std::mem::discriminant(&old)
                                    == std::mem::discriminant(val)
                            }) {
                                new_data.insert(0, old);
                                origin.insert(key, json!(new_data));
                            } else {
                                // Different types, just keep old data
                                origin.insert(key, old);
                            }
                        }
                        (old, new) => {
                            // Only create array if same type
                            if std::mem::discriminant(&old)
                                == std::mem::discriminant(&new)
                                && old != new
                            {
                                origin.insert(key, json!([old, new]));
                            } else {
                                // Keep old value
                                origin.insert(key, old);
                            }
                        }
                    }
                }
            }
        }
        Value::Object(origin)
    }

    fn merge_vec(original: Vec<Value>, new_data: Vec<Value>) -> Value {
        if original.is_empty() {
            Value::Array(new_data)
        } else if new_data.is_empty() {
            Value::Array(original)
        } else {
            // Check that values are the same type. Return array of type original[0]
            let discriminant = std::mem::discriminant(&original[0]);
            let mut filtered: Vec<_> = original
                .into_iter()
                .filter(|v| std::mem::discriminant(v) == discriminant)
                .collect();
            let new: Vec<_> = new_data
                .into_iter()
                .filter(|v| {
                    std::mem::discriminant(v) == discriminant
                        && filtered.iter().all(|val| val != v)
                })
                .collect();
            filtered.extend(new);
            Value::Array(filtered)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rstest::rstest;

        #[rstest]
        #[case(json!("old"), json!("new"), json!(["old", "new"]))]
        #[case(json!(["old1", "old2"]), json!("new"), json!(["old1", "old2", "new"]))]
        #[case(json!("same"), json!("same"), json!("same"))]
        #[case(json!({
            "a": ["An array"],
            "b": 1,
        }), json!({"c": "A string"}), json!({"a": ["An array"], "b": 1, "c": "A string"}))]
        #[case(json!({"a": "Object"}), json!("A string"), json!({"a": "Object"}))]
        #[case(json!("Old string"), json!({"a": 1}), json!("Old string"))]
        fn merging_as_expected(
            #[case] old: Value,
            #[case] new: Value,
            #[case] expected: Value,
        ) {
            let merged = merge_values(old, new);
            assert_eq!(merged, expected);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempdir::TempDir;

    #[test]
    fn failed_to_write_simultaneously() {
        let dir = TempDir::new("writing_test").unwrap();
        let root = dir.path();
        let shared_file = std::sync::Arc::new(AtomicFile::new(&root).unwrap());
        let mut handles = Vec::with_capacity(5);
        for i in 0..5 {
            let file = shared_file.clone();
            let handle = std::thread::spawn(move || {
                let temp = file.make_temp().unwrap();
                let current = file.load().unwrap();
                let content = format!("Content from thread {i}!");
                (&temp).write_all(&content.as_bytes()).unwrap();
                // In case slow computer ensure each thread are running in the same time
                std::thread::sleep(std::time::Duration::from_millis(300));
                file.compare_and_swap(&current, temp)
            });
            handles.push(handle);
        }
        let results = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>();
        // Ensure only one thread has succeed to write
        let success = results.iter().fold(0, |mut acc, r| {
            if r.is_ok() {
                acc += 1;
            }
            acc
        });
        assert_eq!(success, 1);
    }

    #[test]
    fn multiple_writes_detected() {
        let dir = TempDir::new("simultaneous_writes").unwrap();
        let root = dir.path();
        let shared_file = std::sync::Arc::new(AtomicFile::new(&root).unwrap());
        let thread_number = 10;
        assert!(thread_number > 3);
        // Need to have less than 255 thread to store thread number as byte directly
        assert!(thread_number < 256);
        let mut handles = Vec::with_capacity(thread_number);
        for i in 0..thread_number {
            let file = shared_file.clone();
            let handle = std::thread::spawn(move || {
                modify(&file, |data| {
                    let mut data = data.to_vec();
                    data.push(i.try_into().unwrap());
                    data
                })
            });
            handles.push(handle);
        }
        handles.into_iter().for_each(|handle| {
            handle.join().unwrap().unwrap();
        });
        // Last content
        let last_file = shared_file.load().unwrap();
        let last_content = last_file.read_content().unwrap();
        for i in 0..thread_number {
            let as_byte = i.try_into().unwrap();
            assert!(last_content.contains(&as_byte));
        }
    }
}
