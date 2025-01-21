from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')


SERVER_DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(SERVER_DATABASE_URL)

# Define the query
query = """
SELECT * FROM vervotech_mapping 
WHERE ProviderFamily = 'ratehawkhotel' AND country_code = 'SA';
"""

# Execute the query and save the results to a pandas DataFrame
df = pd.read_sql(query, server_engine)

# Save the DataFrame to an Excel file
output_file = "ratehawk_hotels_SA_for_server_vervotech.xlsx"
df.to_excel(output_file, index=False)

print(f"Data has been exported to {output_file}.")


