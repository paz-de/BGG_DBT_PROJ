import snowflake.connector

conn = snowflake.connector.connect(
    user='PAZBEND',
    password='E6Rxta3ehY9qaNG',
    account='ACGKYCE-MU08262',
    warehouse='COMPUTE_WH',
    database='BGG_DB',
    schema='RAW'
)

cur = conn.cursor()
cur.execute("SELECT CURRENT_VERSION()")
print(cur.fetchone())

cur.close()
conn.close()