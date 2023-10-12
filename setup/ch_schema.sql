CREATE TABLE MeterReads (
    MeterID UInt64,
    ChannelID UInt64,
    ReadTimestamp DateTime64(3),
    RawReading Decimal(15,6),
    Status Enum8('Unvalidated' = 0, 'Valid' = 1, 'Estimated' = 2, 'Invalid' = 3, 'Edited' = 4),
    ChangeMethod Enum8('System' = 0, 'Manual' = 1),
    Flag UInt64,
    LastEdited DateTime64(3),
    LastEditedBy String,
)
ENGINE = MergeTree()
PRIMARY KEY (MeterID, ReadTimestamp, LastEdited)
ORDER BY (MeterID, ReadTimestamp, LastEdited);
