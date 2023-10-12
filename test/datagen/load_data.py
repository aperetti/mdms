from clickhouse_connect import get_client
from clickhouse_connect.driver.tools import insert_file
import glob

# Connect to ClickHouse
client = get_client()
# client = False

for file in glob.glob("test/datagen/temp/*.parquet"):
    insert_file(client, "MeterReads", file, "Parquet")

