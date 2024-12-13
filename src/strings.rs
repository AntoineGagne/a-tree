use std::collections::HashMap;

#[derive(Clone, Debug)]
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

    pub fn get_or_update(&mut self, value: &str) -> StringId {
        let counter = self.by_values.entry(value.to_string()).or_insert_with(|| {
            let counter = self.counter;
            self.counter += 1;
            counter
        });

        StringId(*counter)
    }
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Debug, Hash)]
pub struct StringId(usize);

#[cfg(test)]
mod tests {
    use super::*;

    const A_KEY: &str = "test";
    const ANOTHER_KEY: &str = "test_2";

    #[test]
    fn can_get_a_non_existing_string() {
        let table = StringTable::new();

        let id = table.get(A_KEY);

        assert_eq!(id, table.get(ANOTHER_KEY));
    }

    #[test]
    fn update_the_table_with_the_new_string_when_it_is_not_present() {
        let mut table = StringTable::new();

        let id = table.get_or_update(A_KEY);

        assert_eq!(id, table.get(A_KEY));
    }

    #[test]
    fn return_the_same_id_when_the_same_string_is_given() {
        let mut table = StringTable::new();

        let id = table.get_or_update(A_KEY);

        assert_eq!(id, table.get_or_update(A_KEY));
    }

    #[test]
    fn can_add_multiple_strings() {
        let mut table = StringTable::new();

        let id = table.get_or_update(A_KEY);
        let another_id = table.get_or_update(ANOTHER_KEY);

        assert_eq!(id, table.get_or_update(A_KEY));
        assert_eq!(another_id, table.get_or_update(ANOTHER_KEY));
    }
}
