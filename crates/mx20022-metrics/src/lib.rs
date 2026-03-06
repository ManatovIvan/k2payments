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

pub static TRANSACTION_STATE_TRANSITIONS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        prometheus::Opts::new(
            "mx_transaction_state_transitions_total",
            "Total transaction lifecycle state transitions",
        ),
        &["pipeline", "from_state", "to_state"],
    )
    .expect("counter must be constructible");

    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("counter register should work");
    counter
});

pub static PARTICIPANT_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    let hist = HistogramVec::new(
        HistogramOpts::new(
            "mx_participant_duration_seconds",
            "Participant execution duration",
        ),
        &["pipeline", "participant", "action"],
    )
    .expect("histogram must be constructible");

    REGISTRY
        .register(Box::new(hist.clone()))
        .expect("histogram register should work");
    hist
});

pub static PARTICIPANT_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        prometheus::Opts::new(
            "mx_participant_errors_total",
            "Total participant execution errors",
        ),
        &["pipeline", "participant", "error_type"],
    )
    .expect("counter must be constructible");

    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("counter register should work");
    counter
});

pub static RUNTIME_CONFIG_RELOAD_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        prometheus::Opts::new(
            "mx_runtime_config_reload_total",
            "Total runtime participant-config reload attempts",
        ),
        &["result"],
    )
    .expect("counter must be constructible");

    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("counter register should work");
    counter
});

pub static RUNTIME_CONFIG_RELOAD_ERRORS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    let counter = IntCounterVec::new(
        prometheus::Opts::new(
            "mx_runtime_config_reload_errors_total",
            "Total runtime participant-config reload errors",
        ),
        &["error_type"],
    )
    .expect("counter must be constructible");

    REGISTRY
        .register(Box::new(counter.clone()))
        .expect("counter register should work");
    counter
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

pub fn inc_active_transactions(pipeline: &str) {
    TRANSACTIONS_ACTIVE.with_label_values(&[pipeline]).inc();
}

pub fn dec_active_transactions(pipeline: &str) {
    TRANSACTIONS_ACTIVE.with_label_values(&[pipeline]).dec();
}

pub fn record_state_transition(pipeline: &str, from_state: &str, to_state: &str) {
    TRANSACTION_STATE_TRANSITIONS_TOTAL
        .with_label_values(&[pipeline, from_state, to_state])
        .inc();
}

pub fn record_participant_duration(pipeline: &str, participant: &str, action: &str, seconds: f64) {
    PARTICIPANT_DURATION_SECONDS
        .with_label_values(&[pipeline, participant, action])
        .observe(seconds);
}

pub fn record_participant_error(pipeline: &str, participant: &str, error_type: &str) {
    PARTICIPANT_ERRORS_TOTAL
        .with_label_values(&[pipeline, participant, error_type])
        .inc();
}

pub fn record_runtime_config_reload(result: &str) {
    RUNTIME_CONFIG_RELOAD_TOTAL
        .with_label_values(&[result])
        .inc();
}

pub fn record_runtime_config_reload_error(error_type: &str) {
    RUNTIME_CONFIG_RELOAD_ERRORS_TOTAL
        .with_label_values(&[error_type])
        .inc();
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
    use crate::{
        gather, record_participant_duration, record_participant_error,
        record_runtime_config_reload, record_runtime_config_reload_error, record_state_transition,
        record_transaction_duration, record_transaction_total,
    };

    #[test]
    fn metrics_are_exported() {
        record_transaction_total("demo", "pacs.008", "committed");
        record_transaction_duration("demo", "pacs.008", 0.005);
        record_state_transition("demo", "RECEIVED", "PREPARING");
        record_participant_duration("demo", "schema-validator", "prepared", 0.001);
        record_participant_error("demo", "schema-validator", "prepare");
        record_runtime_config_reload("success");
        record_runtime_config_reload_error("parse");

        let snapshot = gather();
        assert!(snapshot.contains("mx_transactions_total"));
        assert!(snapshot.contains("mx_transaction_duration_seconds"));
        assert!(snapshot.contains("mx_transaction_state_transitions_total"));
        assert!(snapshot.contains("mx_participant_duration_seconds"));
        assert!(snapshot.contains("mx_participant_errors_total"));
        assert!(snapshot.contains("mx_runtime_config_reload_total"));
        assert!(snapshot.contains("mx_runtime_config_reload_errors_total"));
    }
}
