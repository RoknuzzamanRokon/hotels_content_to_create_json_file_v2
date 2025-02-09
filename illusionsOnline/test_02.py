from sqlalchemy import MetaData, Table, create_engine, update
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(connection_string)

metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

vervotech_mapping = Table("vervotech_mapping", metadata, autoload_with=engine)


def get_vervotech_mapping_table_all_vervotech_id(session):
    query = session.query(vervotech_mapping.c.ProviderHotelId).all()
    data = {str(row.ProviderHotelId) for row in query} 
    return data

def list_json_file(directory):
    try:
        if not os.path.exists(directory):
            print(f"Error: Directory '{directory}' does not exist.")
            return set()

        json_files = set()
        for f in os.listdir(directory):
            if f.endswith('.json') and os.path.isfile(os.path.join(directory, f)):
                json_files.add(f[:-5].strip().lower())  # Normalize to lowercase and strip spaces

        print(f"Found {len(json_files)} JSON files")  # Debugging output
        return json_files
    except Exception as e:
        print(f"An error occurred while listing JSON files: {e}")
        return set()

def save_results(a, b):
    set_a = {str(item) for item in a}  
    set_b = {str(item) for item in b}  

    result = set_a - set_b  

    results = {"All_get_data_for_new_and_missing_file.txt": result}

    for filename, data in results.items():
        with open(filename, 'w') as f:
            f.write("\n".join(data))

    print("Results saved to respective files.")


if __name__ == "__main__":
    try:
        json_file_path = "D:/content_for_hotel_json/HotelInfo/illusionshotel"
        json_file_list = list_json_file(json_file_path)

        vervotech_mapping_id_list = get_vervotech_mapping_table_all_vervotech_id(session=session)
        save_results(a=json_file_list, b=vervotech_mapping_id_list)
    finally:
        session.close()