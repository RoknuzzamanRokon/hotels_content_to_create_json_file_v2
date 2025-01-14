from sqlalchemy import create_engine, Table, MetaData, insert
from sqlalchemy.orm import sessionmaker
import pandas as pd


DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

metadata.reflect(engine)

illusionsHotel = Table('illusions_online', metadata, autoload=True, autoload_with=engine)
innovativeHotel = Table('innova_hotels_main', metadata, autoload=True, autoload_with=engine)

def transfer_all_data():
    try:
        total_rows = session.query(illusionsHotel).count()
        batch_size = 1000
        total_batches = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)
        for batch in range(total_batches):
            offset = batch * batch_size
            query = session.query(illusionsHotel).limit(batch_size).offset(offset)
            df = pd.read_sql(query, engine)
            rows = df.astype(str).to_dict(orient='records')

            with session.begin():
                for row in rows:
                    keys_to_extract = ['Id', 'hotelCode', 'supplierCode', 'hotelName', 'country', 'latitude', 'longitude', 'address', 'categoryCode', 'giataCode']  
                    filtered_row_dict = {key: row.get(key, None) for key in keys_to_extract}
                    

                    data = {
                        'HotelId': filtered_row_dict.get("hotelCode", None),
                        'GiataCode': filtered_row_dict.get("giataCode", None),
                        'CountryCode': filtered_row_dict.get("country", None),
                        'HotelName': filtered_row_dict.get("hotelName", None),
                        'Latitude': filtered_row_dict.get("latitude", None),
                        'Longitude': filtered_row_dict.get("longitude", None),
                        'AddressLine1': filtered_row_dict.get("address", None),
                        'SupplierCode': filtered_row_dict.get("supplierCode", None)
                    }

                    stmt = insert(innovativeHotel).values(data)
                    session.execute(stmt)
            print(f"Batch {batch + 1} of {total_batches} completed")
        
        print("Data transfer completed")
    except Exception as e:
        print(f"Error occurred: {e}")
        session.rollback()
        session.close()

transfer_all_data()

