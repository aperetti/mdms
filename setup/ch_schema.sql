CREATE TABLE MeterReads (
    MeterID UInt64,
    ChannelID UInt64,
    ReadTimestamp DateTime64(3),
    RawReading Decimal(15,6),
    Status Enum8('Unvalidated' = 0, 'Valid' = 1, 'Estimated' = 2, 'Invalid' = 3, 'Edited' = 4),
    Flag Enum64()
    Version UInt16,
    LastEdited DateTime64(3),
    LastEditedBy String,
    ValidationMetadata Nested(
        RuleName String,
        RuleOutcome Enum8('Pass' = 0, 'Fail' = 1)
    ),
    INDEX MeterID ReadTimestamp TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ReadTimestamp)
ORDER BY (MeterID, ReadTimestamp, Version);
