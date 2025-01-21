from sqlalchemy import create_engine
import requests
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import random


load_dotenv()

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)

GIL_API_KEY = os.getenv("GILL_API_KEY")

class OryxGetDataClass:
    def __init__(self, api_key, url="http://uat-apiv2.giinfotech.ae/api/v2/hotel/hotel-Info"):
        self.api_key = api_key
        self.url = url
        self.headers = {
            'Content-Type': 'application/json',
            'Apikey': f'{self.api_key}'
        }

    def fetch_hotels(self, hotel_id):
        payload = json.dumps({
            "hotelCode": f"{hotel_id}"
        })

        response = requests.post(self.url, headers=self.headers, data=payload)
        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code} - {response.text}")
        return response
    
    def extract_hotel_data(self, response):
        try:
            if response.status_code == 200:
                response = response.json()
                hotel = response.get("hotelInformation",{})


                createdAt = datetime.now()
                createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
                created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
                timeStamp = int(created_at_dt.timestamp())

                address_1 = hotel.get("address", {}).get("line1") or "NULL"
                address_2 = hotel.get("address", {}).get("line2") or "NULL"
                hotel_name = hotel.get("name", "")

                address_query = f"{address_1}, {address_2}, {hotel_name}"
                google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_1 != None else None

                    
                phone_numbers = hotel.get("contact", {}).get("phoneNo") or "NULL"
                email_address = hotel.get("contact", {}).get("email") or "NULL"

                hotel_id = hotel.get("systemId", "")
                hotel_name = hotel.get("name", "")

                
                default_icon = "mdi mdi-alpha-f-circle-outline"



                # Facility data
                master_room_amenities = hotel.get("masterRoomAmenities")
                if master_room_amenities is None:  
                    facility = [
                        {
                            "type": "NULL",
                            "title": "NULL",
                            "icon": "NULL"
                        }
                    ]
                else:
                    facility = [
                        {
                            "type": amenity,
                            "title": amenity,
                            "icon": default_icon
                        }
                        for amenity in master_room_amenities
                    ]


                # Data transformation and default handling
                default_icon = "mdi mdi-alpha-f-circle-outline"

                # Transform hotel photo data
                hotel_photo = [
                    {
                        "picture_id": "NULL",
                        "title": "NULL",
                        "url": photo if photo else "NULL"
                    }
                    for photo in (hotel.get("imageUrls") or [])
                ]

                # Handle case where no photos exist
                if not hotel_photo:
                    hotel_photo = [
                        {
                            "picture_id": "NULL",
                            "title": "NULL",
                            "url": "NULL"
                        }
                    ]

                # Transform amenities data
                amenities = [
                    {
                        "type": amenity,
                        "title": amenity,
                        "icon": default_icon
                    }
                    for amenity in (hotel.get("masterHotelAmenities") or [])
                ]

                # Handle case where no amenities exist
                if not amenities:
                    amenities = [
                        {
                            "type": "NULL",
                            "title": "NULL",
                            "icon": "NULL"
                        }
                    ]


                data = {
                        "created": createdAt_str,
                        "timestamp": timeStamp,
                        "hotel_id": hotel_id,
                        "name": hotel_name,
                        "name_local": hotel_name,
                        "hotel_formerly_name": hotel_name,
                        "destination_code": "NULL",
                        "country_code": hotel.get("address", {}).get("countryCode") or "NULL",
                        "brand_text": "NULL",
                        "property_type": "NULL",
                        "star_rating": hotel.get("rating") or "NULL",
                        "chain": "NULL",
                        "brand": "NULL",
                        "logo": "NULL",
                        "primary_photo": hotel.get("imageUrl") or "NULL",
                        "review_rating": {
                            "source": "N/A",
                            "number_of_reviews": "N/A",
                            "rating_average": "N/A",
                            "popularity_score": "N/A"
                        },
                        "policies": {
                            "check_in": {
                                "begin_time": hotel.get("checkIn") or "NULL",
                                "end_time": hotel.get("checkOut") or "NULL",
                                "instructions": "NULL",
                                "min_age": "NULL"
                            },
                            "checkout": {
                                "time": hotel.get("checkOut") or "NULL",
                            },
                            "fees": {
                                "optional": "NULL",
                            },
                            "know_before_you_go": "NULL",
                            "pets": [
                                "NULL",
                            ],
                            "remark": "NULL",
                            "child_and_extra_bed_policy": {
                                "infant_age": "NULL",
                                "children_age_from": "NULL",
                                "children_age_to": "NULL",
                                "children_stay_free": "NULL",
                                "min_guest_age": "NULL",
                            },
                            "nationality_restrictions": "NULL",
                        },
                        "address": {
                            "latitude": hotel.get("geocode", {}).get("lat") or "NULL",
                            "longitude": hotel.get("geocode", {}).get("lon") or "NULL",
                            "address_line_1": hotel.get("address", {}).get("line1") or "NULL",
                            "address_line_2": hotel.get("address", {}).get("line2") or "NULL",
                            "city": hotel.get("address", {}).get("cityName") or "NULL",
                            "state": hotel.get("address", {}).get("stateName") or "NULL",
                            "stateCode": hotel.get("address", {}).get("stateCode") or "NULL",
                            "country": hotel.get("address", {}).get("countryName") or "NULL",
                            "country_code": hotel.get("address", {}).get("countryCode") or "NULL",
                            "postal_code": hotel.get("address", {}).get("zipCode") or "NULL",
                            "full_address": f"{hotel.get('address', {}).get('line1')}, {hotel.get('address', {}).get('line2')}" or "NULL",
                            "google_map_site_link": google_map_site_link,
                            "local_lang": {
                                    "latitude": hotel.get("geocode", {}).get("lat") or "NULL",
                                    "longitude": hotel.get("geocode", {}).get("lon") or "NULL",
                                    "address_line_1": hotel.get("address", {}).get("line1") or "NULL",
                                    "address_line_2": hotel.get("address", {}).get("line2") or "NULL",
                                    "city": hotel.get("address", {}).get("cityName") or "NULL",
                                    "state": hotel.get("address", {}).get("stateName") or "NULL",
                                    "stateCode": hotel.get("address", {}).get("stateCode") or "NULL",
                                    "country": hotel.get("address", {}).get("countryName") or "NULL",
                                    "country_code": hotel.get("address", {}).get("countryCode") or "NULL",
                                    "postal_code": hotel.get("address", {}).get("zipCode") or "NULL",
                                    "full_address": f"{hotel.get('address', {}).get('line1')}, {hotel.get('address', {}).get('line2')}" or "NULL",
                                    "google_map_site_link": google_map_site_link
                                },
                                "mapping": {
                                    "continent_id": "NULL",
                                    "country_id": "NULL",
                                    "province_id": "NULL",
                                    "state_id": "NULL",
                                    "city_id": "NULL",
                                    "area_id": "NULL",
                                },
                            },
                        "contact": {
                            "phone_number": [phone_numbers],
                            "email_address": [email_address],
                            "fax": hotel.get("contact", {}).get("faxNo") or "NULL",
                            "website": hotel.get("contact", {}).get("website") or "NULL",
                            },
                        "descriptions": "NULL",
                        "room_type": "NULL",
                        "spoken_languages": "NULL",
                        "amenities": amenities,
                        "facilities": facility,
                        "hotel_photo": hotel_photo,
                        "point_of_interests": [
                            {
                                "code": "NULL",
                                "name": "NULL"
                            }
                        ],
                        "nearest_airports": [
                            {
                                "code": "NULL",
                                "name": "NULL"
                            }
                        ],
                        "train_stations": [
                            {
                                "code": "NULL",
                                "name": "NULL"
                            }
                        ],
                        "connected_locations": [
                            {
                                "code": "NULL",
                                "name": "NULL"
                            }
                        ],
                        "stadiums": [
                            {
                                "code": "NULL",
                                "name": "NULL"
                            }
                        ]

                    }
                return data
            else:
                return Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"An error occurred while extracting hotel data:{hotel_id} {str(e)}")








def get_provider_hotel_id_list(engine):
    query = f"SELECT DISTINCT SystemId FROM hotel_info_all;"
    df = pd.read_sql(query, engine)
    data = df['SystemId'].tolist()
    return data




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











def save_json_files_follow_systemId(folder_path, tracking_file_path, cannot_find_file_path, engine):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    systemid_list = get_provider_hotel_id_list(engine)
    print(f"Total System IDs fetched: {len(systemid_list)}")

    initialize_tracking_file(tracking_file_path, systemid_list)

    remaining_ids = read_tracking_file(tracking_file_path)
    print(f"Remaining System IDs to process: {len(remaining_ids)}")

    while remaining_ids:
        systemid = random.choice(list(remaining_ids))  
        file_name = f"{systemid}.json"
        file_path = os.path.join(folder_path, file_name)

        try:
            if os.path.exists(file_path):
                remaining_ids.remove(systemid)
                write_tracking_file(tracking_file_path, remaining_ids)
                print(f"File {file_name} already exists. Skipping...........................Ok")
                continue
            oryx_data = OryxGetDataClass(api_key=GIL_API_KEY)

            hotel_data = oryx_data.fetch_hotels(hotel_id=systemid)
            data_dict = oryx_data.extract_hotel_data(response=hotel_data)


            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(data_dict, json_file, indent=4)

            print(f"Saved {file_name} in {folder_path}")

            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)

        except Exception as e:
            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)
            print(f"Error processing SystemId {systemid}: {e}")
            continue

    try:
        cannot_find_ids = read_tracking_file(cannot_find_file_path)
        remaining_ids = read_tracking_file(tracking_file_path)
        updated_ids = remaining_ids - cannot_find_ids
        write_tracking_file(tracking_file_path, updated_ids)
        print(f"Updated tracking file, removed IDs in 'Cannot find any data' list.")
    except Exception as e:
        remaining_ids.remove(systemid)
        write_tracking_file(tracking_file_path, remaining_ids)
        print(f"Error updating tracking file: {e}")


# 'D:/content_for_
# hotel_json/HotelInfo/TBO'
folder_path = 'D:/Rokon/content_for_hotel_json/HotelInfo/Oryx'
tracking_file_path = 'tracking_file_for_Oryx_content_create.txt'
cannot_find_file_path = 'cannot_find_data_list.txt'

save_json_files_follow_systemId(folder_path, tracking_file_path, cannot_find_file_path, engine)




# oryx_data = OryxGetDataClass(api_key=GIL_API_KEY)

# response = oryx_data.fetch_hotels(hotel_id="481312")
# hotel_data = oryx_data.extract_hotel_data(response=response)

# print(hotel_data)