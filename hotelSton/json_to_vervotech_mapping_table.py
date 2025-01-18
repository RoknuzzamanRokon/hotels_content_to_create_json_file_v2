import os
import requests
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime, timezone
from sqlalchemy.dialects.mysql import insert

class HotelDataProcessor:

    def __init__(self):
        load_dotenv()

        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')

        self.engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}")
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        self.table = Table("vervotech_mapping", self.metadata, autoload_with=self.engine)

    def get_data_from_json(self, supplier_code, hotel_id):
        url = "http://192.168.88.124:5000/hotel_info"

        payload = json.dumps({
            "supplier_code": supplier_code,
            "hotel_id": hotel_id
        })
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def parse_response_data(self, response):
        if response and response.status_code == 200:
            data = response.json()
            return {
                "ProviderFamily": "hotelston",
                "last_update": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "ModifiedOn": datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S.%f"),
                "ProviderHotelId": data.get("hotel_id", "NULL"),
                "status": "Update",
                "hotel_city": data.get("address", {}).get("city", "NULL"),
                "hotel_name": data.get("name", "NULL"),
                "hotel_country": data.get("address", {}).get("country") or None,
                "hotel_longitude": data.get("address", {}).get("longitude", "NULL"),
                "hotel_latitude": data.get("address", {}).get("latitude", "NULL"),
                "country_code": data.get("address", {}).get("country_code", "NULL"),
                "content_update_status": "Done"
            }
        else:
            print("Error: Response data is invalid or status code is not 200.")
            return None

    def upload_data_to_db(self, parsed_data):
        if not parsed_data:
            print("No data to upload.")
            return

        session = self.Session()
        try:
            query = session.query(self.table).filter(
                self.table.c.ProviderFamily == parsed_data["ProviderFamily"],
                self.table.c.ProviderHotelId == parsed_data["ProviderHotelId"]
            )
            
            existing_record = query.first()
            if existing_record:
                print("Record already exists. Skipping insert.")
            else:
                insert_stmt = self.table.insert().values(parsed_data)
                session.execute(insert_stmt)
                session.commit()
                print("Data uploaded successfully.")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Database error: {e}")
        finally:
            session.close()

    def upload_data_to_db_bulk(self, data_batch):
        if not data_batch:
            print("No data to upload.")
            return

        session = self.Session()
        try:
            insert_stmt = insert(self.table).values(data_batch)
            # Use ON DUPLICATE KEY UPDATE for upsert logic
            upsert_stmt = insert_stmt.on_duplicate_key_update(
                last_update=insert_stmt.inserted.last_update,
                ModifiedOn=insert_stmt.inserted.ModifiedOn,
                status=insert_stmt.inserted.status,
                content_update_status=insert_stmt.inserted.content_update_status
            )
            session.execute(upsert_stmt)
            session.commit()
            print(f"Batch of {len(data_batch)} records uploaded successfully.")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Database error: {e}")
        finally:
            session.close()



def initialize_tracking_file(file_path, systemid_list):
    """
    Initializes the tracking file with all SystemIds if it doesn't already exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, systemid_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")

def read_tracking_file(file_path):
    """
    Reads the tracking file and returns a list of remaining SystemIds.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return [line.strip() for line in file.readlines() if line.strip()]

def write_tracking_file(file_path, remaining_ids):
    """
    Updates the tracking file with unprocessed SystemIds.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")

def append_to_cannot_find_file(file_path, systemid):
    """
    Appends the SystemId to the 'Cannot find any data' tracking file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(systemid + "\n")
    except Exception as e:
        print(f"Error appending to 'Cannot find any data' file: {e}")



BATCH_SIZE = 100  

if __name__ == "__main__":
    supplier_code = "hotelston"
    tracking_file_path = "D:/Rokon/hotels_content_to_create_json_file_v2/hotelston/upload_tracking_file_for_upload_data_in_vervotech_table.txt"
    cannot_find_file_path = "D:/Rokon/hotels_content_to_create_json_file_v2/hotelston/cannot_find_data.txt"

    processor = HotelDataProcessor()

    # Read remaining hotel IDs from the tracking file
    remaining_ids = read_tracking_file(tracking_file_path)
    while remaining_ids:
        data_batch = []
        current_batch = remaining_ids[:BATCH_SIZE]
        
        for hotel_id in current_batch:
            print(f"Processing hotel_id: {hotel_id}")
            response = processor.get_data_from_json(supplier_code, hotel_id)
            parsed_data = processor.parse_response_data(response)

            if parsed_data:
                data_batch.append(parsed_data)
            else:
                print(f"Cannot find data for hotel_id: {hotel_id}")
                append_to_cannot_find_file(cannot_find_file_path, hotel_id)

        # Upload the batch to the database
        processor.upload_data_to_db_bulk(data_batch)

        # Remove processed IDs from the tracking file
        remaining_ids = remaining_ids[BATCH_SIZE:]
        write_tracking_file(tracking_file_path, remaining_ids)

        print(f"Remaining IDs to process: {len(remaining_ids)}")

    print("All hotel IDs have been processed.")
