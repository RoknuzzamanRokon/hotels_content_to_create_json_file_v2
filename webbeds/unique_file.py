from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')


SERVER_DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
server_engine = create_engine(SERVER_DATABASE_URL)


metadata = MetaData()
Session = sessionmaker(bind=server_engine)
session = Session()

vervotech_mapping = Table("vervotech_mapping", metadata, autoload_with=server_engine)


def get_all_hotel_id_list(session):
    query = session.query(vervotech_mapping.c.ProviderHotelId).filter(vervotech_mapping.c.ProviderFamily == "DOTW").all()
    data = [row.ProviderHotelId for row in query]
    return data


def list_json_file(directory):
    try:
        json_files = [f[:-5] for f in os.listdir(directory) if f.endswith('.json')]
        return set(json_files)
    except Exception as e:
        print(f"An error occurred while listing JSON files: {e}")
        return set()


def get_unique_entries(list_all_id_from_db, done_json, output_file):
    try:
        if isinstance(list_all_id_from_db, str):
            with open(list_all_id_from_db, 'r') as file1:
                file1_data = {line.strip() for line in file1}
        else:
            file1_data = set(map(str, list_all_id_from_db))

        if isinstance(done_json, str):
            with open(done_json, 'r') as file2:
                file2_data = {line.strip() for line in file2}            
        else:
            file2_data = set(map(str, done_json))

        unique_to_file2 = [item for item in file1_data if item not in file2_data]

        with open(output_file, 'w') as file3:
            file3.write('\n'.join(unique_to_file2))

        print(f"Unique file item is ready. ------------------- {output_file}")

    except Exception as e:
        print(f"An error occurred while getting unique entries: {e}")
        return []


systemid_list_data = get_all_hotel_id_list(session=session)
json_file_path = "D:/content_for_hotel_json/HotelInfo/dotw"
json_file_list = list_json_file(directory=json_file_path)



output_file_name = "final_file_list_not_done.txt"
get_unique_entries(list_all_id_from_db=systemid_list_data, done_json=json_file_list, output_file=output_file_name)