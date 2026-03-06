//! Core runtime primitives: Context, Participant, Transaction Manager, state machine.

pub mod context;
pub mod participant;
pub mod state;
pub mod transaction_manager;

#[cfg(test)]
mod tests_missing;

pub mod prelude {
    pub use crate::context::{Context, ContextAuditEntry, ContextError, ContextMeta};
    pub use crate::participant::{Action, Participant, ParticipantError};
    pub use crate::state::{LifecycleState, StateError};
    pub use crate::transaction_manager::{
        Outcome, ParticipantResult, TransactionError, TransactionManager, TransactionReport,
    };
}
