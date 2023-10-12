import clickhouse_driver

DROP = 1

try:
    client = clickhouse_driver.Client(host="localhost", port=9000, user="default", password="")
    if DROP == 1:
        client.execute("DROP TABLE IF EXISTS MeterReads")
    with open("setup/ch_schema.sql", "r") as f:
        sql = f.read()
        client.execute(sql)
        client.disconnect()
        print("Successfully created ClickHouse schema")
except Exception as e:
    print("Failed to create ClickHouse schema")
    print(e)

