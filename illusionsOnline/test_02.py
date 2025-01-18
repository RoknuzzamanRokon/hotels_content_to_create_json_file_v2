import os
import requests
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from datetime import datetime, timezone


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
                "last_update": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "ModifiedOn": datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S.%f"),
                "ProviderFamily": "illusionshotel",
                "ProviderHotelId": data.get("hotel_id", "NULL"),
                "status": "Update",
                "hotel_city": data.get("address", {}).get("city", "NULL"),
                "hotel_name": data.get("name", "NULL"),
                "hotel_country": data.get("address", {}).get("country", "NULL"),
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
            insert_stmt = self.table.insert().values(parsed_data)
            session.execute(insert_stmt)
            session.commit()
            print("Data uploaded successfully.")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Database error: {e}")
        finally:
            session.close()


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
    Reads the tracking file and returns a set of remaining SystemIds.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return {line.strip() for line in file.readlines()}


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


if __name__ == "__main__":
    supplier_code = "illusionshotel"
    hotel_id = "9999-19654524"

    processor = HotelDataProcessor()

    response = processor.get_data_from_json(supplier_code, hotel_id)
    parsed_data = processor.parse_response_data(response)

    processor.upload_data_to_db(parsed_data)
