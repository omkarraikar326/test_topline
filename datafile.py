import requests
import pandas as pd
from io import BytesIO

# OneDrive file URL
onedrive_url = "https://newsweek0-my.sharepoint.com/:x:/r/personal/o_raikar_newsweek_com/Documents/Azure%20Functions/Azure-Functions-Revenue%20and%20topline%20KPIs%20Newsweek%20Digital%20-%20Copy.xlsx?d=wf65f2d0f72b148f287aebe63eeddccc2&csf=1&web=1&e=ExcXlJ&nav=MTVfezA0OEU1REFFLTY5RUEtNENBOS05REZCLTY0NzMwOUVENUVFQ30"

# Fetch the file
response = requests.get(onedrive_url)
response.raise_for_status()  # Ensure request is successful

# Read CSV or Excel file
# df = pd.read_csv(BytesIO(response.content))  # For CSV
df = pd.read_excel(BytesIO(response.content))  # For Excel

# Function to display dataset info and first few rows
def summarize_data(data):
    print("Dataset Info:")
    print(data.info())
    print("\nFirst 5 Rows:")
    print(data.head())

# Call the function
#summarize_data(df)