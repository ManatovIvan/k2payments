// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    Received,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
    Poison,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid lifecycle transition: {from:?} -> {to:?}")]
pub struct StateError {
    pub from: LifecycleState,
    pub to: LifecycleState,
}

impl LifecycleState {
    pub fn can_transition(self, next: LifecycleState) -> bool {
        use LifecycleState::*;
        matches!(
            (self, next),
            (Received, Preparing)
                | (Preparing, Prepared)
                | (Preparing, Aborting)
                | (Prepared, Committing)
                | (Committing, Committed)
                | (Aborting, Aborted)
                | (Preparing, Poison)
                | (Prepared, Poison)
                | (Committing, Poison)
                | (Aborting, Poison)
        )
    }

    pub fn transition(self, next: LifecycleState) -> Result<LifecycleState, StateError> {
        if self.can_transition(next) {
            Ok(next)
        } else {
            Err(StateError {
                from: self,
                to: next,
            })
        }
    }
}
