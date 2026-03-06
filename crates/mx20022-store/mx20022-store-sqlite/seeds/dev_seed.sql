INSERT INTO transactions (
    tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
) VALUES
(
    'TX-DEV-0001',
    'fednow-credit-transfer',
    'fednow-inbound',
    'pacs.008.001.13',
    '<Document>...</Document>',
    'COMMITTED',
    '1772712000000',
    '1772712002000',
    '{"message_id":"MSG-DEV-0001","end_to_end_id":"E2E-DEV-0001","uetr":"11111111-1111-1111-1111-111111111111"}'
);

INSERT INTO context_mutations (tx_id, key, writer, written_at) VALUES
('TX-DEV-0001', 'routing.destination', 'routing-engine', '1772712001000'),
('TX-DEV-0001', 'status.result', 'status-response-builder', '1772712002000');

INSERT INTO transactions (
    tx_id, pipeline, source_channel, message_type, raw_message, state, received_at, completed_at, key_fields_json
) VALUES
(
    'TX-DEV-0002',
    'fednow-credit-transfer',
    'fednow-inbound',
    'pacs.008.001.13',
    '<Document>...</Document>',
    'POISON',
    '1772712300000',
    '1772712310000',
    '{"message_id":"MSG-DEV-0002","end_to_end_id":"E2E-DEV-0002","uetr":"22222222-2222-2222-2222-222222222222"}'
);

INSERT INTO dead_letters (id, tx_id, reason, failed_at, raw_message) VALUES
('DL-DEV-0001', 'TX-DEV-0002', 'Exceeded retry budget', '1772712310000', '<Document>...</Document>');
