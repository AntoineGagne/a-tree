use std::collections::HashMap;

pub struct StringTable {
    by_values: HashMap<String, usize>,
    counter: usize,
}

impl StringTable {
    const SENTINEL_ID: usize = 0;

    pub fn new() -> Self {
        Self {
            by_values: HashMap::new(),
            counter: 1,
        }
    }

    pub fn get(&self, value: &str) -> StringId {
        let index = self
            .by_values
            .get(value)
            .cloned()
            .unwrap_or(Self::SENTINEL_ID);
        StringId(index)
    }

    fn get_or_update(&mut self, value: &str) -> StringId {
        let counter = self.by_values.entry(value.to_string()).or_insert_with(|| {
            let counter = self.counter;
            self.counter += 1;
            counter
        });

        StringId(*counter)
    }
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct StringId(usize);
