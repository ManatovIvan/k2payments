/// Missing high-value tests for mx20022-runtime-core.
///
/// NOT wired into the build yet.  Add `#[cfg(test)] mod tests_missing;` to
/// lib.rs when ready.
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use async_trait::async_trait;

    use crate::context::{Context, ContextMeta};
    use crate::participant::{Action, Participant, ParticipantError};
    use crate::state::LifecycleState;
    use crate::transaction_manager::{Outcome, TransactionManager};

    // ---------------------------------------------------------------------------
    // Test doubles
    // ---------------------------------------------------------------------------

    struct AlwaysPrepare;

    #[async_trait]
    impl Participant for AlwaysPrepare {
        fn name(&self) -> &str {
            "always-prepare"
        }
        async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
            Ok(Action::Prepared)
        }
    }

    struct FailOnPrepare;

    #[async_trait]
    impl Participant for FailOnPrepare {
        fn name(&self) -> &str {
            "fail-on-prepare"
        }
        async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
            Err(ParticipantError::new("prepare exploded"))
        }
    }

    struct FailOnAbort;

    #[async_trait]
    impl Participant for FailOnAbort {
        fn name(&self) -> &str {
            "fail-on-abort"
        }
        async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
            Ok(Action::Prepared)
        }
        async fn abort(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
            Err(ParticipantError::new("abort exploded"))
        }
    }

    struct FailOnCommit;

    #[async_trait]
    impl Participant for FailOnCommit {
        fn name(&self) -> &str {
            "fail-on-commit"
        }
        async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
            Ok(Action::Prepared)
        }
        async fn commit(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
            Err(ParticipantError::new("commit exploded"))
        }
    }

    fn test_context() -> Context {
        Context::new(ContextMeta {
            transaction_id: "TX-TEST".to_string(),
            received_at: SystemTime::now(),
            pipeline: "test".to_string(),
            source_channel: "inbound".to_string(),
            message_type: "pacs.008".to_string(),
            raw_message: "<Document/>".to_string(),
        })
    }

    // ===========================================================================
    // TEST A: Poison outcome when a commit-phase participant fails (2PC)
    //
    // WHY: This is the most dangerous code path in the entire system.  Once any
    // participant has committed, aborting is impossible — a commit failure leaves
    // the system in an inconsistent state.  The TransactionManager sets
    // LifecycleState::Poison to signal this.  The existing tests only cover the
    // happy path and the prepare-vote-abort path; the commit-failure path
    // (Outcome::Poison) has zero test coverage.
    // ===========================================================================
    #[tokio::test]
    async fn poison_outcome_when_commit_fails_after_prepare() {
        // AlwaysPrepare prepares successfully; FailOnCommit blows up in commit.
        let participants: Vec<Arc<dyn Participant>> =
            vec![Arc::new(AlwaysPrepare), Arc::new(FailOnCommit)];
        let manager = TransactionManager::new(participants);
        let mut ctx = test_context();

        let report = manager
            .process(&mut ctx)
            .await
            .expect("process should not return Err — Poison is an Ok(report) variant");

        assert_eq!(
            report.outcome,
            Outcome::Poison,
            "a commit-phase failure must yield Outcome::Poison"
        );
        assert_eq!(
            ctx.state(),
            LifecycleState::Poison,
            "context state must be Poison after a commit failure"
        );

        // The failing participant must appear in the report.
        let poison_result = report
            .participant_results
            .iter()
            .find(|r| r.participant == "fail-on-commit")
            .expect("fail-on-commit must appear in participant_results");
        assert!(
            poison_result.error.is_some(),
            "participant_result must carry the commit error message"
        );
    }

    // ===========================================================================
    // TEST B: Poison outcome when an abort-phase participant fails (2PC)
    //
    // WHY: abort_prepared iterates previously-prepared participants in reverse
    // and calls abort().  If an abort fails it sets Poison.  This path was never
    // tested, yet it is the escape hatch that prevents a partial rollback from
    // silently appearing as a clean abort.
    // ===========================================================================
    #[tokio::test]
    async fn poison_outcome_when_abort_fails_during_rollback() {
        // FailOnAbort prepares OK but its abort() will throw.
        // A second participant aborts (votes abort in prepare) to trigger rollback.
        struct VoteAbort;
        #[async_trait]
        impl Participant for VoteAbort {
            fn name(&self) -> &str {
                "vote-abort"
            }
            async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
                Ok(Action::Aborted)
            }
        }

        // Ordering: FailOnAbort prepares first, VoteAbort triggers rollback.
        // abort_prepared walks prepared_indexes in reverse — only FailOnAbort
        // is in that list, so its abort() is invoked.
        let participants: Vec<Arc<dyn Participant>> =
            vec![Arc::new(FailOnAbort), Arc::new(VoteAbort)];
        let manager = TransactionManager::new(participants);
        let mut ctx = test_context();

        let report = manager
            .process(&mut ctx)
            .await
            .expect("process should not propagate errors as Err");

        assert_eq!(
            report.outcome,
            Outcome::Poison,
            "an abort-phase failure must yield Outcome::Poison, not Outcome::Aborted"
        );
        assert_eq!(ctx.state(), LifecycleState::Poison);
    }

    // ===========================================================================
    // TEST C: LifecycleState transition guard — invalid transitions are rejected
    //
    // WHY: state.rs defines `can_transition` but has no tests at all.  The
    // transition table is the single source of truth for the 2PC state machine.
    // A wrong entry (e.g., Committed -> Preparing allowed) would let the manager
    // put a context into an incoherent state without anyone noticing.
    // ===========================================================================
    #[test]
    fn state_machine_allows_only_valid_transitions() {
        use LifecycleState::*;

        let valid = [
            (Received, Preparing),
            (Preparing, Prepared),
            (Preparing, Aborting),
            (Preparing, Poison),
            (Prepared, Committing),
            (Prepared, Poison),
            (Committing, Committed),
            (Committing, Poison),
            (Aborting, Aborted),
            (Aborting, Poison),
        ];

        for (from, to) in valid {
            assert!(
                from.can_transition(to),
                "expected valid transition {:?} -> {:?}",
                from,
                to
            );
        }

        // These must all be rejected.
        let invalid = [
            (Received, Committed),
            (Received, Aborted),
            (Committed, Preparing),
            (Committed, Aborting),
            (Aborted, Committing),
            (Poison, Committed),
            (Poison, Aborted),
        ];

        for (from, to) in invalid {
            assert!(
                !from.can_transition(to),
                "expected INVALID transition {:?} -> {:?} to be rejected",
                from,
                to
            );
        }
    }

    // ===========================================================================
    // TEST D: Context value store — type mismatch returns ContextError
    //
    // WHY: Context::get() uses downcast_ref.  If a participant writes a u32 but
    // a later participant reads it as a String, the only signal is
    // ContextError::TypeMismatch.  This is untested — and any change to the
    // boxing/downcasting code would silently break cross-participant data sharing.
    // ===========================================================================
    #[test]
    fn context_get_returns_type_mismatch_error_on_wrong_type() {
        let mut ctx = test_context();
        ctx.put("my-key", 42u32);

        let result = ctx.get::<String>("my-key");
        assert!(
            result.is_err(),
            "reading a u32 as String must return an error"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, crate::context::ContextError::TypeMismatch { .. }),
            "error variant must be TypeMismatch, got: {:?}",
            err
        );
    }

    // ===========================================================================
    // TEST E: prepare-phase error triggers rollback of already-prepared
    //         participants (not just abort-vote path)
    //
    // WHY: The existing test `aborts_when_a_participant_votes_abort` covers the
    // Action::Aborted arm.  The `Err(error)` arm in process() also calls
    // abort_prepared but is never exercised in tests.  A regression that
    // swaps those two branches (e.g., skips abort on prepare error) would not
    // be caught.
    // ===========================================================================
    #[tokio::test]
    async fn prepare_error_triggers_abort_of_previously_prepared_participants() {
        // Track abort calls via an atomic counter shared between the participant
        // and the assertion.
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingAbort {
            counter: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Participant for CountingAbort {
            fn name(&self) -> &str {
                "counting-abort"
            }
            async fn prepare(&self, _ctx: &mut Context) -> Result<Action, ParticipantError> {
                Ok(Action::Prepared)
            }
            async fn abort(&self, _ctx: &mut Context) -> Result<(), ParticipantError> {
                self.counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let abort_counter = Arc::new(AtomicUsize::new(0));

        let participants: Vec<Arc<dyn Participant>> = vec![
            Arc::new(CountingAbort {
                counter: Arc::clone(&abort_counter),
            }),
            Arc::new(FailOnPrepare), // triggers the Err(error) arm
        ];

        let manager = TransactionManager::new(participants);
        let mut ctx = test_context();

        let report = manager
            .process(&mut ctx)
            .await
            .expect("should not return Err");

        // The outcome is Aborted (FailOnPrepare returned Err, not Action::Aborted,
        // but abort_prepared handles both the same way).
        assert_eq!(
            report.outcome,
            Outcome::Aborted,
            "a prepare-phase error should lead to Outcome::Aborted (not Poison), assuming abort succeeds"
        );

        assert_eq!(
            abort_counter.load(Ordering::SeqCst),
            1,
            "CountingAbort.abort() must have been called exactly once for the previously-prepared participant"
        );
    }
}
