from clickhouse_driver import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Create a DataFrame with random data
meters = 10000  # Number of meters to includ
channels = 4 # Number of channels
interval_length = 15 # Number of minutes
duration = 365 # Number of days to generate
batch_size = 1e3 # number of meters per batch

end_date = datetime.now().date()

range = pd.date_range(end_date + timedelta(days=-duration),datetime.now(), end_date, freq="15T")

dates = np.repeat(range.values, channels * meters)
meter_ids = np.repeat(np.arange(meters) + 1000000, channels * range),
channel_ids = np.repeat(np.arange(meters * channels) + 1000000, range)

data = np.flatten(np.random.randint(0, 1000, dates.size))
status = np.flatten(np.random.choice(['Valid', 'Estimated', 'Invalid', 'Edited'], size=data.size, p=[.99, .007, .0005, .0025]))

version_mask = np.random.choice([True, False], size=data.size, p=[.01, .99])
dates_version = dates[version_mask]
meters_version = meters[version_mask]
channel_version = channel_ids[version_mask]
data_version = data[version_mask]
status_version =status[version_mask]

version2_mask = version_mask & np.random.choice([True, False], size=data.size, p=[.01, .99])
dates_version2 = dates[version2_mask]
meters_version2 = meters[version2_mask]
channel_version2 = channel_ids[version2_mask]
data_version2 = data[version2_mask]
status_version2 =status[version2_mask]

# Generate synthetic data
data = {
    'MeterID': np.concat([meter_ids, meters_version, meters_version2]),
    'ChannelID': np.concat([channel_ids, channel_version, channel_version2]),
    'ReadTimestamp': np.concat([dates, dates_version, dates_version2]),
    'RawReading': np.concat([data, data_versin, data_version2]),
    'Flag': [random.choice(['Unvalidated', 'Valid', 'Estimated', 'Invalid', 'Edited']) for _ in range(num_rows)],
    'Version': [random.randint(1, 5) for _ in range(num_rows)],
    'LastEdited': [end_date + timedelta(seconds=random.randint(0, 100)) for _ in range(num_rows)],
    'LastEditedBy': [random.choice(['System', 'User1', 'User2']) for _ in range(num_rows)],
    'ValidationMetadata': [[{'RuleName': 'Check1', 'RuleOutcome': 'Pass'}] for _ in range(num_rows)]
}

df = pd.DataFrame(data)

# Connect to ClickHouse
client = Client(host='localhost', port=9000, user='default', password='')

# Create a table in ClickHouse if not exists
client.execute("""
CREATE TABLE IF NOT EXISTS MeterReads (
    MeterID UInt64,
    ChannelID UInt64,
    ReadTimestamp DateTime64(3),
    RawReading Decimal(15,6),
    Flag Enum8('Unvalidated' = 0, 'Valid' = 1, 'Estimated' = 2, 'Invalid' = 3, 'Edited' = 4),
    Version UInt16,
    LastEdited DateTime64(3),
    LastEditedBy String,
    ValidationMetadata Nested(
        RuleName String,
        RuleOutcome Enum8('Pass' = 0, 'Fail' = 1)
    )
) ENGINE = MergeTree() PARTITION BY toYYYYMM(ReadTimestamp) ORDER BY (MeterID, ReadTimestamp, Version);
""")

# Convert the DataFrame to a list of tuples
records = df.to_records(index=False).tolist()

# Insert data into ClickHouse
client.execute('INSERT INTO MeterReads (MeterID, ChannelID, ReadTimestamp, RawReading, Flag, Version, LastEdited, LastEditedBy, ValidationMetadata) VALUES', records)
