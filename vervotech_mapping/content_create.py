import requests
import json
from datetime import datetime 
import os
import random

class VervotechHotelData:
    def __init__(self, url, account_id, api_key):
        self.url = url
        self.headers = {
            'accountid': account_id,
            'apikey': api_key,
            'Content-Type': 'application/json'
        }

    def fetch_hotel_data(self, vervotech_ids, provider_preferences):
        payload = json.dumps({
            "ProviderHotelIdentifiers": [
                {
                    "ProviderHotelId": vervotech_ids[0], 
                    "ProviderFamily": provider_preferences[0]
                }
            ]
        })

        response = requests.post(self.url, headers=self.headers, data=payload)
        response.raise_for_status()  
        return response.json()
    
    
    def create_content_follow_hotel_id(self, data):
        hotels = []
        for hotel in data.get("Hotels", []):
            for provider_hotel in hotel.get("ProviderHotels", []):

                createdAt = datetime.now()
                createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
                created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
                timeStamp = int(created_at_dt.timestamp())


                address_line_1 = provider_hotel.get("Contact", {}).get("Address", {}).get("Line1") or "NULL"
                address_line_2 = provider_hotel.get("Contact", {}).get("Address", {}).get("Line2") or "NULL"
                hotel_name = provider_hotel.get("Name") or "NULL",
                address_query = f"{address_line_1}, {address_line_2}, {hotel_name}"
                google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None

                descriptions = [
                        {
                            "title": desc.get("Type", "NULL"),
                            "text": desc.get("Text", "NULL")
                        }
                        for desc in provider_hotel.get("Descriptions", [])
                    ]


                # Transforming the data with .get() method
                hotel_photos = []

                for image in provider_hotel.get("Images", []):
                    for link in image.get("Links", []):
                        hotel_photos.append({
                            "picture_id": "NULL",  
                            "title": image.get("Category", ""), 
                            "url": link.get("ProviderHref", "") 
                        })
                        


                # Point of interests.
                point_of_interests = []
                for poi in provider_hotel.get("PointOfInterests", []):
                    point_of_interests.append({
                        "code": poi.get("Geocode", ""),  
                        "name": poi.get("Name", "") 
                    })

                    
                
                # Transforming the data facilities.
                facilities = []
                for facility in provider_hotel.get("Facilities", []):
                    facilities.append({
                        "type": facility.get("Name", ""),  
                        "title": facility.get("GroupName", ""),  
                        "icon": "mdi mdi-alpha-f-circle-outline"
                    })

                data = {
                    "createdAt": createdAt_str,
                    "timeStamp": timeStamp,
                    "hotel_id": provider_hotel.get("ProviderHotelId") or "NULL",
                    "name": provider_hotel.get("Name") or "NULL",
                    "name_local": provider_hotel.get("Name") or "NULL",
                    "name_formerly_name": provider_hotel.get("Name") or "NULL",
                    "destination_code": provider_hotel.get("Contact", {}).get("Address", {}).get("DestinationCode") or "NULL",
                    "country_code": provider_hotel.get("Contact", {}).get("Address", {}).get("CountryCode") or "NULL",
                    "brand_text": provider_hotel.get("BrandName") or "NULL",
                    "property_type": provider_hotel.get("PropertyType") or "NULL",
                    "star_rating": provider_hotel.get("Rating") or "NULL",
                    "chain": provider_hotel.get("ChainName") or "NULL",
                    "brand:": "NULL",
                    "logo": "NULL",
                    "primary_photo": provider_hotel.get("ProviderHeroImageHref") or "NULL",
                    "review_rating": {
                        "source": "N/A",
                        "number_of_reviews": provider_hotel.get("Reviews") or "NULL",
                        "rating_average": "N/A",
                        "popularity": provider_hotel.get("PopularityScore") or "NULL",
                        },
                    "policies": {
                        "checkin": {
                            "begin_time": provider_hotel.get("Checkin", {}).get("BeginTime") or "NULL",
                            "end_time": provider_hotel.get("Checkin", {}).get("EndTime") or "NULL",
                            "instructions":  provider_hotel.get("Checkin", {}).get("Instructions") or "NULL",
                            "special_instructions": provider_hotel.get("Checkin", {}).get("SpecialInstructions") or "NULL",
                            "min_age": provider_hotel.get("Checkin", {}).get("MinAge") or "NULL",
                            },
                        "checkout": {
                            "time": provider_hotel.get("Checkout", {}).get("Time") or "NULL",
                            },
                        "fees": {
                            "optional": provider_hotel.get("Fees", {}) or "NULL",
                            "mandatory": provider_hotel.get("Fees", {}) or "NULL",
                            },
                        "know_before_you_go": "NULL",
                        "pets": "NULL",
                        "remark": "NULL",
                        "child_and_extra_bed_policy": {
                            "infant_age": "NULL",
                            "children_age_from": "NULL",
                            "children_age_to": "NULL",
                            "children_stay_free": "NULL",
                            "min_guest_age": "NULL"
                            },
                        "nationality_restrictions": "NULL",
                        },
                        "address": {
                            "latitude": provider_hotel.get("GeoCode", {}).get("Lat") or "NULL",
                            "longitude": provider_hotel.get("GeoCode", {}).get("Long") or "NULL",
                            "address_line_1": provider_hotel.get("Contact", {}).get("Address", {}).get("Line1") or "NULL",
                            "address_line_2": provider_hotel.get("Contact", {}).get("Address", {}).get("Line2") or "NULL",
                            "city": provider_hotel.get("Contact", {}).get("Address", {}).get("City") or "NULL",
                            "state": provider_hotel.get("Contact", {}).get("Address", {}).get("State") or "NULL",
                            "country": provider_hotel.get("Contact", {}).get("Address", {}).get("Country") or "NULL",
                            "country_code": provider_hotel.get("Contact", {}).get("Address", {}).get("CountryCode") or "NULL",
                            "postal_code": provider_hotel.get("Contact", {}).get("Address", {}).get("PostalCode") or "NULL",
                            "full_address": f"{provider_hotel.get("Contact", {}).get("Address", {}).get("Line1") or "NULL"}, {provider_hotel.get("Contact", {}).get("Address", {}).get("Line2") or "NULL"}",
                            "google_map_link": google_map_site_link,
                            "local_lang": {
                                "latitude": provider_hotel.get("GeoCode", {}).get("Lat") or "NULL",
                                "longitude": provider_hotel.get("GeoCode", {}).get("Long") or "NULL",
                                "address_line_1": provider_hotel.get("Contact", {}).get("Address", {}).get("Line1") or "NULL",
                                "address_line_2": provider_hotel.get("Contact", {}).get("Address", {}).get("Line2") or "NULL",
                                "city": provider_hotel.get("Contact", {}).get("Address", {}).get("City") or "NULL",
                                "state": provider_hotel.get("Contact", {}).get("Address", {}).get("State") or "NULL",
                                "country": provider_hotel.get("Contact", {}).get("Address", {}).get("Country") or "NULL",
                                "country_code": provider_hotel.get("Contact", {}).get("Address", {}).get("CountryCode") or "NULL",
                                "postal_code": provider_hotel.get("Contact", {}).get("Address", {}).get("PostalCode") or "NULL",
                                "full_address": f"{provider_hotel.get("Contact", {}).get("Address", {}).get("Line1") or "NULL"}, {provider_hotel.get("Contact", {}).get("Address", {}).get("Line2") or "NULL"}",
                                "google_map_link": google_map_site_link
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
                        "contacts": {
                            "phone_numbers": provider_hotel.get("Contact", {}).get("Phones"),
                            "fax": provider_hotel.get("Contact", {}).get("Fax"),
                            "email_address": provider_hotel.get("Contact", {}).get("Emails"),
                            "website": provider_hotel.get("Contact", {}).get("Web"),
                        },
                        "descriptions": descriptions,
                        "room_type": "NULL",
                        "sponken_language": "NULL",
                        "facilities": facilities,
                        "hotel_photo": hotel_photos,
                        "point_of_interests": point_of_interests,
                        "nearest_airports": "NULL",
                        "train_stations": "NULL",
                        "connected_locations": "NULL",
                        "stadiums": "NULL"
                }
                hotels.append(data)
        return hotels






def initialize_tracking_file(file_path, systemid_list):
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, systemid_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")


def read_tracking_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return {line.strip() for line in file.readlines()}


def write_tracking_file(file_path, remaining_ids):
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")


def append_to_cannot_find_file(file_path, systemid):
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(systemid + "\n")
    except Exception as e:
        print(f"Error appending to 'Cannot find any data' file: {e}")


def save_json_files_follow_systemId(folder_path, tracking_file_path):
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

            url = "https://hotelmapping.vervotech.com/api/3.0/content/GetProviderContentByProviderHotelIds"
            account_id = "gtrs"
            api_key = "b0ae90d7-2507-4751-ba4d-d119827c1ed2"
            vervotech_ids = [systemid]
            provider_preferences = ["DOTW"]

            vervotech_object = VervotechHotelData(url, account_id, api_key)
            response_data = vervotech_object.fetch_hotel_data(vervotech_ids, provider_preferences)
            data_list = vervotech_object.create_content_follow_hotel_id(response_data)
            
            if not data_list:
                print(f"Data not found for Hotel ID: {systemid}. Skipping...")
                append_to_cannot_find_file("cannot_find_file.txt", systemid)
                remaining_ids.remove(systemid)
                write_tracking_file(tracking_file_path, remaining_ids)
                continue

            # Ensure the folder exists before saving
            os.makedirs(folder_path, exist_ok=True)

            with open(file_path, "w", encoding="utf-8") as json_file:
                json.dump(data_list, json_file, indent=4)

            print(f"Saved {file_name} in {folder_path}")
            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)

        except Exception as e:
            remaining_ids.remove(systemid)
            write_tracking_file(tracking_file_path, remaining_ids)
            print(f"Error processing SystemId {systemid}: {e}")
            continue


# Path settings
folder_path = 'D:/content_for_hotel_json/HotelInfo/webbeds'
tracking_file_path = 'final_file_list_not_done.txt'

save_json_files_follow_systemId(folder_path, tracking_file_path)