use rand::{thread_rng, Rng};
use std::time::Duration;
use tracing::trace;

pub(crate) type DurationFloat = f64;

pub(crate) struct BackoffSleep {
    min: DurationFloat,
    max: DurationFloat,
    last: DurationFloat,
    mult: DurationFloat,
}

impl BackoffSleep {
    pub(crate) fn new(min: DurationFloat, max: DurationFloat, mult: DurationFloat) -> Self {
        if mult <= 1.0 {
            panic!("`mult` should be greater than 1.0 to make backoff interval increasing")
        }

        if min >= max {
            panic!("`max` should be greater than `min` to make backoff interval increasing")
        }

        if min <= 0.0 || max <= 0.0 {
            panic!("`min` and `max` should be greater than zero")
        }

        Self {
            min,
            max,
            mult,
            last: min,
        }
    }

    pub(crate) fn reset(&mut self) {
        trace!("reset backoff state");
        self.last = self.min;
    }

    pub(crate) async fn sleep(&mut self) {
        let duration = self.next();
        trace!(?duration, "backoff sleep");
        tokio::time::sleep(duration).await;
    }

    fn next(&mut self) -> Duration {
        self.last = self.random();
        trace!(duration = ?self.last, "next random duration requested");
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

        let val = thread_rng().gen_range(min..=max);
        trace!(
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
    use std::borrow::BorrowMut;

    use super::*;
    use crate::manager::tests::init_tracing;

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

    #[test]
    fn ensure_long_less_then_max() {
        const MIN: DurationFloat = 0.1;
        const MAX: DurationFloat = 10.0;
        const MULT: f64 = 2.0;

        init_tracing();

        let mut backoff = BackoffSleep::new(MIN, MAX, MULT);
        let v: Vec<Duration> = (0..55).borrow_mut().map(|_| backoff.next()).skip(50).collect();
        for d in v {
            assert!(d <= Duration::from_secs_f64(MAX));
            assert!(d >= Duration::from_secs_f64(MAX / MULT));
        }
    }

    #[test]
    #[should_panic = "`mult` should be greater than 1.0 to make backoff interval increasing"]
    fn incorrect_backoff_mult_equal_to_one() {
        let _ = BackoffSleep::new(1.0, 2.0, 1.0);
    }

    #[test]
    #[should_panic = "`mult` should be greater than 1.0 to make backoff interval increasing"]
    fn incorrect_backoff_mult_less_than_one() {
        let _ = BackoffSleep::new(1.0, 2.0, 0.999);
    }

    #[test]
    #[should_panic = "`max` should be greater than `min` to make backoff interval increasing"]
    fn incorrect_backoff_min_greater_than_max() {
        let _ = BackoffSleep::new(3.0, 2.0, 2.0);
    }

    #[test]
    #[should_panic = "`min` and `max` should be greater than zero"]
    fn incorrect_backoff_min_or_max_less_then_zero() {
        let _ = BackoffSleep::new(0.0, 2.0, 2.0);
        let _ = BackoffSleep::new(0.0, 0.0, 2.0);
    }
}
