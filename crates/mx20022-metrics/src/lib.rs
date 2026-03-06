use once_cell::sync::Lazy;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Registry, TextEncoder,
};

static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static TRANSACTIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        prometheus::Opts::new("mx_transactions_total", "Total number of transactions"),
        &["pipeline", "message_type", "outcome"],
    )
    .expect("counter must be constructible");

    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("counter register should work");
    counter
});

pub static TRANSACTION_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    let hist = HistogramVec::new(
        HistogramOpts::new(
            "mx_transaction_duration_seconds",
            "Transaction processing duration",
        ),
        &["pipeline", "message_type"],
    )
    .expect("histogram must be constructible");

    REGISTRY
        .register(Box::new(hist.clone()))
        .expect("histogram register should work");
    hist
});

pub static TRANSACTIONS_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    let gauge = IntGaugeVec::new(
        prometheus::Opts::new("mx_transactions_active", "Active in-flight transactions"),
        &["pipeline"],
    )
    .expect("gauge must be constructible");

    REGISTRY
        .register(Box::new(gauge.clone()))
        .expect("gauge register should work");
    gauge
});

pub fn record_transaction_total(pipeline: &str, message_type: &str, outcome: &str) {
    TRANSACTIONS_TOTAL
        .with_label_values(&[pipeline, message_type, outcome])
        .inc();
}

pub fn record_transaction_duration(pipeline: &str, message_type: &str, seconds: f64) {
    TRANSACTION_DURATION_SECONDS
        .with_label_values(&[pipeline, message_type])
        .observe(seconds);
}

pub fn set_active_transactions(pipeline: &str, value: i64) {
    TRANSACTIONS_ACTIVE
        .with_label_values(&[pipeline])
        .set(value);
}

pub fn gather() -> String {
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();

    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .expect("encoding metrics should work");

    String::from_utf8(buffer).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use crate::{gather, record_transaction_duration, record_transaction_total};

    #[test]
    fn metrics_are_exported() {
        record_transaction_total("demo", "pacs.008", "committed");
        record_transaction_duration("demo", "pacs.008", 0.005);

        let snapshot = gather();
        assert!(snapshot.contains("mx_transactions_total"));
        assert!(snapshot.contains("mx_transaction_duration_seconds"));
    }
}
