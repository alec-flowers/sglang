//! The batches a relay retains so a consumer can resume from a cursor.

use std::collections::VecDeque;

/// A window `[oldest_seq, next_seq)` of consecutive batches, bounded by count
/// and by bytes. Sequence numbers inside the window are dense.
pub struct History<T> {
    entries: VecDeque<(T, usize)>,
    next_seq: u64,
    bytes: usize,
    max_batches: usize,
    max_bytes: usize,
}

impl<T> History<T> {
    pub fn new(max_batches: usize, max_bytes: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            next_seq: 0,
            bytes: 0,
            max_batches,
            max_bytes,
        }
    }

    /// Append the batch numbered `next_seq`, evicting the oldest batches that
    /// no longer fit. A batch larger than the byte budget is not retained.
    pub fn push(&mut self, item: T, bytes: usize) {
        self.entries.push_back((item, bytes));
        self.bytes += bytes;
        self.next_seq += 1;
        while self.entries.len() > self.max_batches || self.bytes > self.max_bytes {
            let Some((_, evicted)) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= evicted;
        }
    }

    pub fn oldest_seq(&self) -> u64 {
        self.next_seq - self.entries.len() as u64
    }

    /// The sequence number the next pushed batch gets.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Whether a consumer that has applied every batch before `seq` can be
    /// brought up to date from the window alone.
    pub fn covers(&self, seq: u64) -> bool {
        self.oldest_seq() <= seq && seq <= self.next_seq
    }

    /// The retained batches in `[from, end)`.
    pub fn range(&self, from: u64, end: u64) -> impl Iterator<Item = &T> {
        let oldest = self.oldest_seq();
        let start = from.clamp(oldest, self.next_seq) - oldest;
        let end = end.clamp(oldest, self.next_seq) - oldest;
        self.entries
            .range(start as usize..end.max(start) as usize)
            .map(|(item, _)| item)
    }

    /// Forget everything; the window becomes the empty `[next_seq, next_seq)`.
    pub fn reset(&mut self, next_seq: u64) {
        self.entries.clear();
        self.bytes = 0;
        self.next_seq = next_seq;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seqs(history: &History<u64>, from: u64, end: u64) -> Vec<u64> {
        history.range(from, end).copied().collect()
    }

    #[test]
    fn window_tracks_both_caps_and_keeps_sequence_numbers() {
        // Each item is its own sequence number.
        let mut history = History::new(3, 100);
        for seq in 0..5 {
            history.push(seq, 10);
        }
        assert_eq!((history.oldest_seq(), history.next_seq()), (2, 5));
        assert_eq!(seqs(&history, 0, 10), [2, 3, 4]);
        assert_eq!(seqs(&history, 3, 4), [3]);
        assert_eq!(seqs(&history, 5, 5), [0u64; 0]);
        // A consumer exactly at the tail is covered; one before the window is not.
        assert!(history.covers(5) && history.covers(2));
        assert!(!history.covers(1) && !history.covers(6));

        // One 95-byte batch pushes out all but itself under the byte cap.
        history.push(5, 95);
        assert_eq!(seqs(&history, 0, 10), [5]);
        // A batch over the whole budget is not retained, but still numbered.
        history.push(6, 101);
        assert_eq!((history.oldest_seq(), history.next_seq()), (7, 7));

        history.reset(20);
        history.push(20, 1);
        assert_eq!(seqs(&history, 0, 100), [20]);
    }
}
