from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Source and Target Database URLs
DATABASE_URL_LOCAL_1 = "mysql+pymysql://root:@localhost/csvdata01_02102024"
DATABASE_URL_LOCAL_2 = "mysql+pymysql://root:@localhost/innova_db_v1.25"

# Create SQLAlchemy engines for source and target databases
local_engine_1 = create_engine(DATABASE_URL_LOCAL_1)
local_engine_2 = create_engine(DATABASE_URL_LOCAL_2)


def insert_data_in_chunks(engine_source, engine_target, page_size, start_offset):
    """
    Inserts data from one table into another in chunks, skipping rows that already exist.
    Parameters:
        engine_source (Engine): SQLAlchemy engine for the source database.
        engine_target (Engine): SQLAlchemy engine for the target database.
        page_size (int): Number of rows to fetch and insert in each batch.
        start_offset (int): Initial offset to start fetching rows.
    """
    print("Starting data insertion process...")

    # Query to fetch data from the source table
    select_query = text("""
        SELECT * FROM innova_hotels_main
        WHERE SupplierCode = 'ratehawkhotel'
        LIMIT :limit OFFSET :offset
    """)

    # Query to insert data into the target table
    insert_query = text("""
        INSERT INTO global_hotel_list (
            supplier_code, hotel_id, VervotechId, GiataCode, destination_code,
            hotel_name, latitude, longitude, primary_photo, country_code, country, city_code, city, state,
            state_code, postal_code, address_line_1, address_line_2, star_rating, website, email_address,
            phone_numbers, fax
        )
        VALUES (
            :supplier_code, :hotel_id, :VervotechId, :GiataCode, :destination_code,
            :hotel_name, :latitude, :longitude, :primary_photo, :country_code, :country, :city_code, :city, :state,
            :state_code, :postal_code, :address_line_1, :address_line_2, :star_rating, :website, :email_address,
            :phone_numbers, :fax
        )
    """)

    # Query to check if a row already exists in the target table
    check_query = text("""
        SELECT 1 FROM global_hotel_list 
        WHERE supplier_code = :supplier_code AND hotel_id = :hotel_id
        LIMIT 1
    """)

    try:
        offset = start_offset
        chunk_count = 1

        while True:
            with engine_source.connect() as source_conn:
                result = source_conn.execute(select_query, {'limit': page_size, 'offset': offset}).fetchall()

                if not result:
                    print("No more records to transfer.")
                    break

                print(f"Fetched {len(result)} records from the source database.")
                offset += page_size

                data_to_insert = []
                with engine_target.connect() as target_conn:
                    for row in result:
                        # Check if the row already exists
                        exists = target_conn.execute(check_query, {
                            'supplier_code': row[4],
                            'hotel_id': row[5]
                        }).fetchone()

                        if not exists:
                            # Add the row to the insert list if it doesn't exist
                            data_to_insert.append({
                                'supplier_code': row[4],
                                'hotel_id': row[5],
                                'VervotechId': row[2],
                                'GiataCode': row[3],
                                'destination_code': row[6],
                                'hotel_name': row[15],
                                'latitude': row[16],
                                'longitude': row[17],
                                'primary_photo': row[18],
                                'country_code': row[13],
                                'country': row[12],
                                'city_code': row[8],
                                'city': row[7],
                                'state': row[9],
                                'state_code': row[10],
                                'postal_code': row[11],
                                'address_line_1': row[19],
                                'address_line_2': row[20],
                                'star_rating': row[26],
                                'website': row[22],
                                'email_address': row[23],
                                'phone_numbers': row[24],
                                'fax': row[25]
                            })

                if data_to_insert:
                    try:
                        with engine_target.begin() as target_conn:
                            target_conn.execute(insert_query, data_to_insert)
                            print(f"Chunk {chunk_count} inserted successfully.")
                            chunk_count += 1
                    except SQLAlchemyError as e:
                        print(f"Error during insert operation for chunk {chunk_count}: {e}")
                        continue  

    except SQLAlchemyError as e:
        print(f"Error during database operation: {e}")

    print("Data insertion completed.")


# Start the process with a specific offset and chunk size
insert_data_in_chunks(local_engine_1, local_engine_2, page_size=1000, start_offset=1054000)
