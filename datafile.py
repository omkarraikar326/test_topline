import pyodbc
import pandas as pd

# Define connection parameters
server = "wu7lzrrt26be7oqcdktogact2m-qgj64uch2nfevir5hqdfapui7a.datawarehouse.fabric.microsoft.com"
database = "prod_nw_lakehouse001"  # Replace with actual database name

# Use Active Directory Interactive Authentication (prompts for login)
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Authentication=ActiveDirectoryInteractive;"
)

# Connect to SQL Server (triggers an authentication popup)
conn = pyodbc.connect(conn_str)

# SQL Query to fetch latest data
query = """
SELECT * FROM prod_nw_lakehouse001.gold_topline_kpi
WHERE Date = (SELECT MAX(Date) FROM prod_nw_lakehouse001.gold_topline_kpi)
"""

# Load data into Pandas DataFrame
df = pd.read_sql(query, conn)

# Close the connection
conn.close()

# Display Data
print(df.head())