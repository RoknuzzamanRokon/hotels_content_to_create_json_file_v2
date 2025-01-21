import pandas as pd
from sqlalchemy import create_engine

# Replace these with your database connection details
DATABASE_URI = "mysql+pymysql://root:@localhost/csvdata01_02102024"

# Create a connection to the database
engine = create_engine(DATABASE_URI)

# Define the query
query = """
SELECT * FROM innova_hotels_main 
WHERE SupplierCode = 'ratehawkhotel' AND CountryCode = 'SA';
"""

# Execute the query and save the results to a pandas DataFrame
df = pd.read_sql(query, engine)

# Save the DataFrame to an Excel file
output_file = "ratehawk_hotels_SA_for_local_iit_table.xlsx"
df.to_excel(output_file, index=False)

print(f"Data has been exported to {output_file}.")
