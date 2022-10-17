import snowflake.connector
import os

# Gets the version
ctx = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_LOGIN"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_HOST"),
)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()
