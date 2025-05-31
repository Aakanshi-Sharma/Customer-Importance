CREATE TABLE IF NOT EXISTS offsets (
    key TEXT PRIMARY KEY,
    value INTEGER
);

INSERT INTO offsets (key, value) VALUES ('transactions_offset', 0)
ON CONFLICT (key) DO NOTHING;
