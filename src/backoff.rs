use rand::{thread_rng, Rng};
use std::time::Duration;
use tracing::debug;

pub(crate) type DurationFloat = f64;

pub(crate) struct BackoffSleep {
    min: DurationFloat,
    max: DurationFloat,
    last: DurationFloat,
    mult: DurationFloat,
}

impl BackoffSleep {
    pub(crate) fn new(min: DurationFloat, max: DurationFloat, mult: DurationFloat) -> Self {
        Self {
            min,
            max,
            mult,
            last: min,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.last = self.min;
    }

    pub(crate) async fn sleep(&mut self) {
        tokio::time::sleep(self.next()).await;
    }

    fn next(&mut self) -> Duration {
        self.last = self.random();
        Duration::from_secs_f64(self.last)
    }

    fn random(&self) -> DurationFloat {
        let min = self.last;
        let max = min * self.mult;

        let (min, max) = if max > self.max {
            (self.max / self.mult, self.max)
        } else {
            (min, max)
        };

        let val = thread_rng().gen_range(min..max);
        debug!(
            min = format!("{min:.3}"),
            val = format!("{val:.3}"),
            max = format!("{max:.3}"),
            "generate next random duration"
        );

        val
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init_tracing;

    #[test]
    fn ensure_every_next_is_longer() {
        init_tracing();

        let mut backoff = BackoffSleep::new(0.1, 10.0, 2.0);
        let mut prev = Duration::ZERO;

        for _ in 0..5 {
            let next = backoff.next();
            assert!(next > prev);
            prev = next;
        }
    }
}
