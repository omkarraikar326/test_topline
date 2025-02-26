from sqlalchemy import create_engine
import pandas as pd

# Paste your copied Lakehouse connection string
connection_string = "wu7lzrrt26be7oqcdktogact2m-qgj64uch2nfevir5hqdfapui7a.datawarehouse.fabric.microsoft.com"

# Create SQLAlchemy engine
engine = create_engine(connection_string)

# Query your Lakehouse data
query = "SELECT MAX(Date) AS max_date FROM prod_nw_lakehouse001.gold_topline_kpi"
df = pd.read_sql(query, engine)

# Print results
print(df)
