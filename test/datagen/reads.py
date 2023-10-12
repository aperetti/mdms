from clickhouse_connect import get_client
from decimal import Decimal
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# Connect to ClickHouse
client = get_client()
# client = False


def random_data(data):
    # Generates random data assuming the followin, modifying the input data in place
    #  - Channel #1 is KWh.Delivered
    #  - Channel #2 is KWh.Received
    #  - Channel #3 is KVarh.Delivered
    #  - Channel #4 is Voltage
    data[0::4] = np.round(np.random.normal(100, 10, size=dates.size // 4), 3)
    data[1::4] = 0
    data[2::4] = np.round(data[0::4] * 10 + np.random.normal(1, .5, size=dates.size // 4), 3)
    data[3::4] = np.round(120 + np.random.normal(1, 1, size=dates.size // 4), 1)

# Create a DataFrame with random data
METERS = 20000  # Number of meters to includ
CHANNELS = 4  # Number of channels
INTERVAL_LENGTH = 15  # Number of minutes
DURATION = 365  # Number of days to generate
BATCH_SIZE = int(.5e3)  # umber of meters per batch

end_date = datetime.now().date()

dt_range = pd.date_range(
    end_date + timedelta(days=-DURATION), end_date, freq="15T"
).values

dates = np.tile(dt_range, CHANNELS * BATCH_SIZE)
edit_offset = np.ones(dates.size, dtype=np.int64) + int(1e12)
meter_ids = np.repeat(np.arange(BATCH_SIZE) + 1000000, CHANNELS * dt_range.size)
channel_ids = np.repeat(np.arange(BATCH_SIZE * CHANNELS) + 1000000, dt_range.size)
data = np.ones(dates.size)
random_data(data)

status = np.random.choice(
    [0, 1, 2, 3],
    size=data.size,
    p=[0.99, 0.007, 0.0005, 0.0025],
).flatten()

df = pd.DataFrame(
    {
        "MeterID": meter_ids,
        "ChannelID": channel_ids,
        "ReadTimestamp": dates,
        "RawReading": np.asarray(data, dtype=Decimal),
        "Status": status,
        "ChangeMethod": 0,
        "Flag": np.zeros(data.size).astype(np.int64),
        "LastEdited": dates + pd.to_timedelta("1T"),
        "LastEditedBy": "System",
    }
)

# Geneterate a second version of the data
mask_v1 = np.random.choice([True, False], size=data.size, p=[0.01, 0.99])

df_v1 = df[mask_v1].copy()
df_v1.loc[:, "RawReading"] = df_v1["RawReading"] + 1
df_v1.loc[:, "Status"] = 1
df_v1.loc[:, "LastEdited"] = df_v1["LastEdited"] + pd.to_timedelta("1T")

# Generate a third version of the data
mask_v2 = mask_v1 & np.random.choice([True, False], size=data.size, p=[0.01, 0.99])

df_v2 = df[mask_v2].copy()
df_v2.loc[:, "RawReading"] = df_v2["RawReading"] + 1
df_v2.loc[:, "Status"] = 1
df_v2.loc[:, "ChangeMethod"] = 1
df_v2.loc[:, "LastEdited"] = df_v2["LastEdited"] + pd.to_timedelta("2T")

df = pd.concat([df, df_v1, df_v2])

for i in range(BATCH_SIZE, METERS, BATCH_SIZE):
    # Convert the DataFrame to a list of tuples
    df.to_parquet(f"test/datagen/temp/reads-{i}.parquet", index=False)
    # if client is not False:
    #     # Insert data into ClickHouse
    #     client.insert_dataframe(
    #         "INSERT into MeterReads VALUES"
    #         , df
    #     )

    df["MeterID"] = df["MeterID"] + BATCH_SIZE
    df["ChannelID"] = df["ChannelID"] + BATCH_SIZE * CHANNELS
    