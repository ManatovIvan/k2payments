// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    last_refill: Instant,
    last_seen: Instant,
}

#[derive(Debug)]
pub struct AdminRateLimiter {
    rate_per_second: f64,
    burst: f64,
    buckets: Mutex<HashMap<String, BucketState>>,
}

impl Default for AdminRateLimiter {
    fn default() -> Self {
        Self::new(20.0, 40.0)
    }
}

impl AdminRateLimiter {
    pub fn new(rate_per_second: f64, burst: f64) -> Self {
        Self {
            rate_per_second: rate_per_second.max(0.1),
            burst: burst.max(1.0),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    pub fn allow(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        if buckets.len() > 2048 {
            let stale_after = Duration::from_secs(10 * 60);
            buckets.retain(|_, bucket| now.duration_since(bucket.last_seen) < stale_after);
        }

        let bucket = buckets
            .entry(key.to_string())
            .or_insert_with(|| BucketState {
                tokens: self.burst,
                last_refill: now,
                last_seen: now,
            });

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            bucket.tokens = (bucket.tokens + elapsed * self.rate_per_second).min(self.burst);
            bucket.last_refill = now;
        }
        bucket.last_seen = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AdminRateLimiter;

    #[test]
    fn token_bucket_rejects_after_burst_is_exhausted() {
        let limiter = AdminRateLimiter::new(0.1, 2.0);
        assert!(limiter.allow("key"));
        assert!(limiter.allow("key"));
        assert!(!limiter.allow("key"));
    }
}
