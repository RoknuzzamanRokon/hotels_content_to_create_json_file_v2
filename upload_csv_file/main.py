import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError

# File path to your CSV file
csv_file = "D:/Rokon/gtrs_mappings_2b547cba-9ee9-4bcd-a09e-806ae724df3f.csv"

# Define the data types for the columns
dtype_dict = {
    'UnicaId': str,
    'ProviderHotelId': str,
    'ProviderFamily': str,
    'ModifiedOn': str,  # We'll handle datetime conversion separately
    'ChannelIds': str,
    'ProviderLocationCode': str
}

# Load the CSV file with the specified data types
df = pd.read_csv(csv_file, dtype=dtype_dict, low_memory=False)

# Specify the date format explicitly to avoid warnings
date_format = "%m/%d/%Y %I:%M:%S %p"  # Adjust this format based on your actual date format

# Convert 'ModifiedOn' column to datetime format with the specified format
df['ModifiedOn'] = pd.to_datetime(df['ModifiedOn'], format=date_format, errors='coerce')  # Handle date parsing issues

# Replace NaN values with 'NULL' only for NaN fields (not for valid values)
df = df.apply(lambda col: col.map(lambda x: 'NULL' if pd.isna(x) else x))

# Ensure 'ModifiedOn' is converted to a string in a format compatible with SQL
df['ModifiedOn'] = df['ModifiedOn'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Check the dataframe after transformations
print("Data after transformations:")
print(df.head())

# Create SQLAlchemy engine and connect to MySQL
DATABASE_URL = "mysql+pymysql://root:@localhost/innova_db_v1.25"
engine = create_engine(DATABASE_URL)

# Create the table if it doesn't exist
metadata = MetaData()

# Define the table with the correct argument for autoload
gtrs_mappings = Table(
    'gtrs_mappings', metadata,
    autoload_with=engine  # Use autoload_with to load the table's schema
)

# Prepare the insert statement
insert_query = gtrs_mappings.insert()

# Set the chunk size for batch insertion
chunk_size = 1000

# Begin transaction
with engine.connect() as conn:
    try:
        # Collect all rows to insert into a list of dictionaries (matching column names)
        rows_to_insert = df.to_dict(orient='records')
        
        # Debugging: Print the number of rows to insert
        print(f"Total rows to insert: {len(rows_to_insert)}")

        # Iterate over the rows in chunks
        for i in range(0, len(rows_to_insert), chunk_size):
            chunk = rows_to_insert[i:i+chunk_size]
            
            # Insert the chunk into the database
            try:
                conn.execute(insert_query, chunk)
                conn.commit()  # Commit after each chunk
                print(f"Successfully inserted rows {i+1} to {i+len(chunk)}.")
            except SQLAlchemyError as e:
                print(f"Error during insertion: {e}")
                conn.rollback()  # Rollback if any error occurs

        print("Data insertion completed successfully.")
    
    except SQLAlchemyError as e:
        print(f"Error during transaction: {e}")
        conn.rollback()  # Rollback in case of an error

    finally:
        print("Transaction completed.")

# Close the engine connection
engine.dispose()
