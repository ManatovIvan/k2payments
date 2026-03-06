DROP INDEX IF EXISTS idx_expectations_state_timeout;
DROP INDEX IF EXISTS idx_context_mutations_tx_written;
DROP INDEX IF EXISTS idx_transactions_state_received;
DROP INDEX IF EXISTS idx_transactions_msg_type_received;

DROP TABLE IF EXISTS dead_letters;
DROP TABLE IF EXISTS expectations;
DROP TABLE IF EXISTS context_mutations;
DROP TABLE IF EXISTS transactions;
