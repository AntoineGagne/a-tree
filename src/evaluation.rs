#[derive(Debug)]
pub struct EvaluationResult {
    failed: Vec<u64>,
    success: Vec<u64>,
    evaluated: Vec<u64>,
}

impl EvaluationResult {
    const EXPRESSIONS_PER_BUCKET: usize = 64;

    pub fn new(expressions: usize) -> Self {
        let size = expressions / Self::EXPRESSIONS_PER_BUCKET;
        Self {
            failed: vec![0; size],
            success: vec![0; size],
            evaluated: vec![0; size],
        }
    }

    #[inline]
    pub fn is_evaluated(&self, id: usize) -> bool {
        let evaluated = Self::get_bit(&self.evaluated, id);
        evaluated != 0u64
    }

    #[inline]
    pub fn set_result(&mut self, id: usize, result: Option<bool>) {
        match result {
            Some(true) => {
                Self::set_bit(&mut self.success, id);
            }
            Some(false) => {
                Self::set_bit(&mut self.failed, id);
            }
            None => {}
        }

        Self::set_bit(&mut self.evaluated, id);
    }

    #[inline]
    pub fn get_result(&self, id: usize) -> Option<bool> {
        debug_assert!(self.is_evaluated(id));
        let failed = Self::get_bit(&self.failed, id) != 0u64;
        let success = Self::get_bit(&self.success, id) != 0u64;
        if !failed && !success {
            return None;
        }
        Some(!failed && success)
    }

    #[inline]
    const fn set_bit(entries: &mut [u64], id: usize) {
        let position_in_entry: usize = id % Self::EXPRESSIONS_PER_BUCKET;
        entries[id / Self::EXPRESSIONS_PER_BUCKET] |= 1u64 << position_in_entry;
    }

    #[inline]
    const fn get_bit(entries: &[u64], id: usize) -> u64 {
        let entry = entries[id / Self::EXPRESSIONS_PER_BUCKET];
        let position_in_entry: usize = id % Self::EXPRESSIONS_PER_BUCKET;
        entry & (1u64 << position_in_entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIZE: usize = 128;
    const AN_ID: usize = 1;
    const AN_ID_THAT_EXCEEDS_U64: usize = 67;

    #[test]
    fn when_looking_if_unevaluated_result_is_evaluated_then_return_false() {
        let results = EvaluationResult::new(SIZE);

        assert!(!results.is_evaluated(AN_ID));
    }

    #[test]
    fn when_looking_if_evaluated_result_is_evaluated_then_return_true() {
        let mut results = EvaluationResult::new(SIZE);

        results.set_result(AN_ID, Some(false));

        assert!(results.is_evaluated(AN_ID));
    }

    #[test]
    fn can_set_a_successful_result() {
        let mut results = EvaluationResult::new(SIZE);

        results.set_result(AN_ID, Some(true));

        assert!(results.is_evaluated(AN_ID));
        assert_eq!(Some(true), results.get_result(AN_ID));
    }

    #[test]
    fn can_set_a_failed_result() {
        let mut results = EvaluationResult::new(SIZE);

        results.set_result(AN_ID, Some(false));

        assert!(results.is_evaluated(AN_ID));
        assert_eq!(Some(false), results.get_result(AN_ID));
    }

    #[test]
    fn can_set_an_undefined_result() {
        let mut results = EvaluationResult::new(SIZE);

        results.set_result(AN_ID, None);

        assert!(results.is_evaluated(AN_ID));
        assert_eq!(None, results.get_result(AN_ID));
    }

    #[test]
    fn can_set_id_that_exceeds_u64() {
        let mut results = EvaluationResult::new(SIZE);

        results.set_result(AN_ID_THAT_EXCEEDS_U64, Some(false));

        assert!(results.is_evaluated(AN_ID_THAT_EXCEEDS_U64));
        assert_eq!(Some(false), results.get_result(AN_ID_THAT_EXCEEDS_U64));
    }
}
