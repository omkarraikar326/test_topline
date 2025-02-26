import pyodbc
import pandas as pd

# Paste your SQL connection string here
connection_string = "wu7lzrrt26be7oqcdktogact2m-qgj64uch2nfevir5hqdfapui7a.datawarehouse.fabric.microsoft.com"

# Extract details from the connection string
conn = pyodbc.connect(connection_string)

# SQL Query to fetch latest data
query = """
SELECT * FROM prod_nw_lakehouse001.data
WHERE Date = (SELECT MAX(Date) FROM prod_nw_lakehouse001.data)
"""

# Load data into Pandas DataFrame
df = pd.read_sql(query, conn)

# Close the connection
#conn.close()

# Display Data
print(df.head())