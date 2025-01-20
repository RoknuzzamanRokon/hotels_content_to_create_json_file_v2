import os

def list_json_file(directory, output_file):
    try:
        json_files = [f[:-5] for f in os.listdir(directory) if f.endswith('.json')]

        with open(output_file, 'w') as file:
            for name in json_files:
                file.write(f"{name}\n")
        print(f"File list has been written to {output_file}")
    except Exception as e:
        print(f"An error occurred: {e}")



directory = "D:/content_for_hotel_json/HotelInfo/ratehawkhotel"
file_path = "new.txt"

list_json_file(directory=directory, output_file=file_path)