import pandas as pd
from snowflake.connector import SnowflakeConnection
import snowflake.connector

from typing import Any  # optional, if you need a fallback
import os
from dotenv import load_dotenv

load_dotenv()
ENV = os.environ


def get_snowflake_connection_browser_auth() -> SnowflakeConnection:

    """Creates a live connection to the Snowflake database using a browser authentication.

    Returns

    -------

    connection: A live connection to the Snowflake database.

    """

    return  snowflake.connector.connect(     

        user=ENV["USER_NG_EMAIL"],     

        authenticator='externalbrowser',     

        account=ENV["SNOWFLAKE_ACCOUNT"],

        warehouse=ENV["SNOWFLAKE_WAREHOUSE"],  

        database=ENV["SNOWFLAKE_DATABASE"],     

        schema=ENV["SNOWFLAKE_SCHEMA"]

    )


def extract_data_from_table(con, table_name) -> pd.DataFrame:
    """
    Extracts all data from the specified table and returns it as a Pandas DataFrame.
    Parameters
    ----------
    con: Active Snowflake connection object.
    table_name: Name of the table to extract data from.
    Returns
    -------
    pd.DataFrame:  DataFrame containing all rows and columns from the specified table.
    """
    # warehouse = ENV["SNOWFLAKE_WAREHOUSE"]
    #use fetch many
 
    with con.cursor() as curs:
        curs.execute("""SELECT *FROM AOCONC_PROD.DATASETS.VISUAL_INSPECTIONS_RESULTS_RS_V3 LIMIT 100""")
        rows = curs.fetchall()
        columns = [desc[0].lower() for desc in curs.description]
        return pd.DataFrame(rows, columns=columns)
    
con = get_snowflake_connection_browser_auth()

cursor = con.cursor()
cursor.execute("SELECT CURRENT_DATABASE()")

print("Database:", cursor.fetchone())

cursor.execute("SELECT CURRENT_SCHEMA()")

print("Schema:", cursor.fetchone())

cursor.execute("SELECT CURRENT_ROLE()")

print("Role:", cursor.fetchone())

df = extract_data_from_table(con, "VISUAL_INSPECTIONS_RESULTS_RS_V3")
print(df)

df.to_excel(r'C:\Users\robert.everitt\OneDrive - National Grid\Data Engineering Course\snowflake_test.xlsx')
