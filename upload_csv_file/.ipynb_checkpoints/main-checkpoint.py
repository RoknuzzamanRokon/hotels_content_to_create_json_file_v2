import pandas as pd
import mysql.connector
from mysql.connector import Error

dtype_dict = {
    'UnicaId': str,
    'ProviderHotelId': str,
    'ProviderFamily': str,
    'ModifiedOn': str,  
    'ChannelIds': str,
    'ProviderLocationCode': str
}

csv_file = "D:/csv01_02102024/Vervotech-Mapping-2024-09-21/gtrs_mappings_a5a093d7-a4f5-4063-9b4c-38facb3084d8.csv"

df = pd.read_csv(csv_file, dtype=dtype_dict)

# Convert ModifiedOn to datetime
df['ModifiedOn'] = pd.to_datetime(df['ModifiedOn'], format='%m/%d/%Y %I:%M:%S %p')

# Fill NaN values - Set string columns to None for SQL NULL representation
df = df.where(pd.notnull(df), None)

header = df.columns.tolist()
print("Header:", header)
# print("Length of header:", len(header))
# print(df)  

try:
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",  
        database="csvdata01_02102024"
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # Create the info_01 table if it doesn't exist
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS info_01 (
            UnicaId VARCHAR(255),
            ProviderHotelId VARCHAR(255),
            ProviderFamily VARCHAR(255),
            ModifiedOn DATETIME,
            ChannelIds VARCHAR(255),
            ProviderLocationCode VARCHAR(255)
        );
        '''

        cursor.execute(create_table_query)

        # Prepare the insert statement for the info_01 table
        insert_query = '''INSERT INTO info_01 (UnicaId, ProviderHotelId, ProviderFamily, ModifiedOn, ChannelIds, ProviderLocationCode)
                          VALUES (%s, %s, %s, %s, %s, %s)'''

        num = 0
        # Iterate over DataFrame rows and insert data
        for i, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
            connection.commit()  
            print(f"{num}: Successfully loaded.")
            num += 1

        print("Data inserted successfully")
except Error as e:
    print(f"Error while connecting to MySql: {e}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySql connection closed")
