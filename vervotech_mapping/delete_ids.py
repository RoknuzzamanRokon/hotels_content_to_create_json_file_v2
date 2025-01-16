from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')

# Database connection URL
SERVER_DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(SERVER_DATABASE_URL)

# Initialize metadata and session
metadata = MetaData()
Session = sessionmaker(bind=server_engine)
session = Session()

# Reflect the table structure
vervotech_mapping = Table("vervotech_mapping", metadata, autoload_with=server_engine)

# List of file paths containing ProviderHotelIds
file_paths =  ["D:/Rokon/hotels_content_to_create_json_file_v2/vervotech_mapping/cannot_find_file.txt"]

try:
    for file_path in file_paths:
        # Read ProviderHotelIds from file
        with open(file_path, "r") as file:
            provider_hotel_ids = [line.strip() for line in file if line.strip()]

        # Search for matching IDs in the database
        with server_engine.connect() as connection:
            select_query = vervotech_mapping.select().where(vervotech_mapping.c.ProviderHotelId.in_(provider_hotel_ids))
            result = connection.execute(select_query).fetchall()
            matching_ids = [row[0] for row in result]  # Access by index, assuming ProviderHotelId is the first column

            if matching_ids:
                # Perform bulk delete for matching IDs
                delete_query = vervotech_mapping.delete().where(vervotech_mapping.c.ProviderHotelId.in_(matching_ids))
                connection.execute(delete_query)
                print(f"Deleted {len(matching_ids)} records from {file_path}.")
            else:
                print(f"No matching records found to delete in {file_path}.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the session
    session.close()
