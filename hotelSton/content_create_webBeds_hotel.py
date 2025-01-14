import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import os
from datetime import datetime 
import pandas as pd  
from sqlalchemy import create_engine
import json
import random

load_dotenv()

class DOTWHotelSearch:
    def __init__(self, url, username, password, user_id, headers):
        """
        Initializes the hotel search class with required credentials and parameters.
        """
        self.url = url
        self.username = username
        self.password = password
        self.user_id = user_id
        self.headers = headers

    def create_payload(self, city_code):
        """
        Generates the XML payload for the hotel search request.
        """
        return f"""<customer>
            <username>{self.username}</username>
            <password>{self.password}</password>
            <id>{self.user_id}</id>
            <source>1</source>
            <product>hotel</product>
            <language>en</language>
            <request command="searchhotels">
                <bookingDetails>
                    <fromDate>2024-01-01</fromDate>
                    <toDate>2024-10-05</toDate>
                    <currency>416</currency> 
                    <rooms no="5">
                        <room runno="0">
                            <adultsCode>1</adultsCode>
                            <children no="0"></children>
                            <rateBasis>-1</rateBasis>
                        </room>
                    </rooms>
                </bookingDetails>
                <return>
                    <getRooms>true</getRooms> 
                    <filters xmlns:a="http://us.dotwconnect.com/xsd/atomicCondition" xmlns:c="http://us.dotwconnect.com/xsd/complexCondition">
                        <city>{city_code}</city>
                        <noPrice>true</noPrice>
                    </filters>
                        <fields>
                            <field>preferred</field>  
                            <field>builtYear</field>  
                            <field>renovationYear</field>  
                            <field>floors</field>  
                            <field>noOfRooms</field>  
                            <field>preferred</field>  
                            <field>fullAddress</field>  
                            <field>description1</field>  
                            <field>description2</field>  
                            <field>hotelName</field>  
                            <field>address</field>  
                            <field>zipCode</field>  
                            <field>location</field>  
                            <field>locationId</field>  
                            <field>geoLocations</field>  
                            <field>location1</field>  
                            <field>location2</field>  
                            <field>location3</field>  
                            <field>cityName</field>  
                            <field>cityCode</field>  
                            <field>stateName</field>  
                            <field>stateCode</field>  
                            <field>countryName</field>  
                            <field>countryCode</field>  
                            <field>regionName</field>  
                            <field>regionCode</field>  
                            <field>attraction</field>  
                            <field>amenitie</field>  
                            <field>leisure</field>  
                            <field>business</field>  
                            <field>transportation</field>  
                            <field>hotelPhone</field>  
                            <field>hotelCheckIn</field>  
                            <field>hotelCheckOut</field>  
                            <field>minAge</field>  
                            <field>rating</field>  
                            <field>images</field>  
                            <field>fireSafety</field>  
                            <field>hotelPreference</field>  
                            <field>direct</field>  
                            <field>geoPoint</field>  
                            <field>leftToSell</field>  
                            <field>chain</field>  
                            <field>lastUpdated</field>  
                            <field>priority</field>  
                            <roomField>name</roomField>  
                            <roomField>roomInfo</roomField>  
                            <roomField>roomAmenities</roomField>  
                            <roomField>twin</roomField>  
                        </fields>
                </return>
            </request>
        </customer>"""

    def fetch_hotel_data(self, city_code):
        """
        Sends the request to the DOTW API and retrieves the response.
        """
        payload = self.create_payload(city_code)
        try:
            response = requests.post(self.url, headers=self.headers, data=payload)
            if response.status_code == 200:
                try:
                    return ET.fromstring(response.text) 
                except ET.ParseError as e:
                    print(f"XML ParseError for City ID {city_code}: {e}")
                    return None
            else:
                print(f"Error: Received status code {response.status_code} for City ID {city_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for City ID {city_code}: {e}")
            return None

    def parse_hotel_data(self, root):
        """
        Parses the XML response and extracts hotel information.
        """
        hotels = root.find(".//hotels")

        createdAt = datetime.now()
        createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
        created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
        timeStamp = int(created_at_dt.timestamp())
        

        if hotels is not None:
            hotel_list = []
            for hotel in hotels.findall("hotel"):


                # Extract primary photo and hotel photos
                primary_photo = "N/A"
                hotel_photos = []

                # Find the hotelImages element
                hotel_images = hotel.find("images/hotelImages")

                if hotel_images is not None:
                    thumb = hotel_images.find("thumb")
                    if thumb is not None and thumb.text:
                        primary_photo = thumb.text.strip()

                    for image in hotel_images.findall("image"):
                        category = image.find("category")
                        picture_id = category.get("id") if category is not None else "N/A"
                        
                        alt_tag = image.find("alt")
                        if alt_tag is not None and alt_tag.text:
                            title = alt_tag.text
                        elif category is not None:
                            title = category.text
                        else:
                            title = "N/A"  
                        
                        url_tag = image.find("url")
                        url = url_tag.text.strip() if url_tag is not None and url_tag.text else "N/A"

                        hotel_photos.append({
                            "picture_id": picture_id,
                            "title": title,
                            "url": url
                        })




                # Extract latitude and longitude from <geoPoint>
                geo_point = hotel.find("geoPoint")
                latitude = "N/A"
                longitude = "N/A"
                if geo_point is not None:
                    latitude = geo_point.find("lat").text if geo_point.find("lat") is not None else "N/A"
                    longitude = geo_point.find("lng").text if geo_point.find("lng") is not None else "N/A"



                # Extract full address details
                full_address = hotel.find("fullAddress")
                address = "N/A"
                zip_code = "N/A"
                country = "N/A"
                state = "N/A"
                city = "N/A"
                if full_address is not None:
                    address = full_address.find("hotelStreetAddress").text if full_address.find("hotelStreetAddress") is not None else "N/A"
                    zip_code = full_address.find("hotelZipCode").text if full_address.find("hotelZipCode") is not None else "N/A"
                    country = full_address.find("hotelCountry").text if full_address.find("hotelCountry") is not None else "N/A"
                    state = full_address.find("hotelState").text if full_address.find("hotelState") is not None else "N/A"
                    city = full_address.find("hotelCity").text if full_address.find("hotelCity") is not None else "N/A"



                # Genarate data for google links.
                address_line_1 = address
                hotel_name = hotel.find("hotelName").text if hotel.find("hotelName") is not None else "N/A",
                address_query = f"{address_line_1}, {hotel_name}"
                google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None


                # Extract description1 details
                description1 = hotel.find("description1/language[@id='EN']")
                description_info = {
                    "title": "Description",
                    "text": "N/A"
                }
                if description1 is not None and description1.text:
                    description_info["text"] = description1.text.strip()

                amenities_list = []
                amenities = hotel.find("amenitie")

                if amenities is not None:
                    for amenity_item in amenities.findall("language/amenitieItem"):
                        amenity_name = amenity_item.text.strip() if amenity_item.text else "N/A"
                        amenity_dict = {
                            "type": amenity_name,  
                            "title": amenity_name,  
                            "icon": "mdi mdi-alpha-f-circle-outline"  
                        }
                        amenities_list.append(amenity_dict)
                else:
                    amenities_list.append({
                        "type": "N/A",
                        "title": "N/A",
                        "icon": "mdi mdi-alpha-f-circle-outline"
                    })


                # Initialize the point_of_interests list
                formatted_pointOfInterests = []

                # Extract the geoLocations element
                geo_locations = hotel.find("geoLocations")

                if geo_locations is not None:
                    for geo_location in geo_locations.findall("geoLocation"):
                        code = geo_location.get("id", "N/A") 
                        name = geo_location.find("name").text if geo_location.find("name") is not None else "N/A" 

                        formatted_pointOfInterests.append({
                            "code": code,
                            "name": name
                        })


                # Initialize lists for nearest airports and train stations
                nearest_airports = []
                train_stations = []

                transportation = hotel.find("transportation")

                if transportation is not None:
                    airports = transportation.find("airports")
                    if airports is not None:
                        nearest_airports.append({
                            "code": "N/A",
                            "name": airports.get("name", "N/A")  
                        })
                    
                    rails = transportation.find("rails")
                    if rails is not None:
                        train_stations.append({
                            "code": "N/A",
                            "name": rails.get("name", "N/A")  
                        })




                    # Initialize lists for nearest airports and train stations
                    nearest_airports = []
                    train_stations = []

                    transportation = hotel.find("transportation")
                    if transportation is not None:
                        # Extract nearest airports
                        airports = transportation.find("airports")
                        if airports is not None:
                            nearest_airports.append({
                                "code": "N/A", 
                                "name": airports.get("name", "N/A") 
                            })
                        
                        rails = transportation.find("rails")
                        if rails is not None:
                            train_stations.append({
                                "code": "N/A", 
                                "name": rails.get("name", "N/A")  
                            })




                hotel_data = {
                    "created": createdAt_str,
                    "timestamp": timeStamp,
                    "hotel_id": hotel.get("hotelid", "N/A"),
                    "name": hotel.find("hotelName").text if hotel.find("hotelName") is not None else "N/A",
                    "name_local": hotel.find("hotelName").text if hotel.find("hotelName") is not None else "N/A",
                    "name_formerly_name": hotel.find("hotelName").text if hotel.find("hotelName") is not None else "N/A",
                    "destination_code": None,
                    "country_code": hotel.find("countryCode").text if hotel.find("countryCode") is not None else "N/A",
                    "brand_text": None,
                    "property_type": None,
                    "star_rating": None,     
                    "chain": hotel.find("chain").text if hotel.find("chain") is not None else "N/A",
                    "brand": None,
                    "logo": None,
                    "primary_photo": primary_photo,

                    "review_rating": {
                            "source": "N/A",
                            "number_of_reviews": hotel.find("rating").text if hotel.find("rating") is not None else "N/A",
                            "rating_average": None,
                            "popularity_score": None
                        },

                    "policies": {
                        "checkin": {
                            "begin_time": hotel.find("hotelCheckIn").text if hotel.find("hotelCheckIn") is not None else "N/A",
                            "end_time": hotel.find("hotelCheckOut").text if hotel.find("hotelCheckOut") is not None else "N/A",
                            "instructions": None,
                            "special_instructions": None,
                            "min_age":  None,
                            },
                        "checkout": {
                            "time": hotel.find("hotelCheckOut").text if hotel.find("hotelCheckOut") is not None else "N/A",
                            },
                        "fees": {
                            "optional": None,
                            "mandatory": None,
                            },
                        "know_before_you_go": None,
                        "pets": None,
                        "remark": None,
                        "child_and_extra_bed_policy": {
                            "infant_age": None,
                            "children_age_from": None,
                            "children_age_to": None,
                            "children_stay_free": None,
                            "min_guest_age": None
                            },
                        "nationality_restrictions": None,
                        },

                    "address": {
                        "latitude": latitude,
                        "longitude": longitude,
                        "address_line_1": address,
                        "address_line_2": None,
                        "city": city,
                        "state": state,
                        "country": country,
                        "country_code": hotel.find("countryCode").text if hotel.find("countryCode") is not None else "N/A",
                        "postal_code": zip_code,
                        "full_address": f"{address}",
                        "google_map_site_link": google_map_site_link,
                        "local_lang": {
                            "latitude": latitude,
                            "longitude": longitude,
                            "address_line_1": address,
                            "address_line_2": None,
                            "city": city,
                            "state": state,
                            "country": country,
                            "country_code": hotel.find("countryCode").text if hotel.find("countryCode") is not None else "N/A",
                            "postal_code": zip_code,
                            "full_address": f"{address}",
                            "google_map_site_link": google_map_site_link,
                            },
                        "mapping": {
                            "continent_id": None,
                            "country_id": None,
                            "province_id": None,
                            "state_id": None,
                            "city_id": None,
                            "area_id": None
                            }
                        },


                    "contacts": {
                        "phone_numbers": hotel.find("hotelPhone").text if hotel.find("hotelPhone") is not None else "N/A",
                        "fax": None,
                        "email_address": None,
                        "website": None,
                        },

                    "description": description_info,
                    "room_type": None,
                    "spoken_languages": None,
                    "amenities": amenities_list,
                    "facilities": None,
                    "hotel_photo": hotel_photos, 
                    "point_of_interests": formatted_pointOfInterests,
                    "nearest_airports": nearest_airports,
                    "train_stations": train_stations,
                    "connected_locations": None,
                    "stadiums": None


                }
                hotel_list.append(hotel_data)
            return hotel_list
        else:
            print("No hotels found.")
            return []

    def get_hotel_data_as_json(self, hotels):
        """
        Returns the hotel data in JSON format.
        """
        if hotels:
            return json.dumps(hotels, indent=4)
        else:
            return json.dumps({"message": "No hotels available to display."}, indent=4)






def get_city_id_list(engine, table):
    query = f"SELECT CityCode FROM {table};"
    df = pd.read_sql(query, engine)
    data = df['CityCode'].tolist()
    return data


def initialize_tracking_file(file_path, city_id_list):
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, city_id_list)) + "\n")
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


def save_json_files_follow_systemId(folder_path, tracking_file_path):
    # Database credentials
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    DATABASE_URL = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
    engine = create_engine(DATABASE_URL)

    # Create folder if not exists
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Initialize tracking
    table = "all_city_name"
    city_id_list = get_city_id_list(engine=engine, table=table)
    print(f"Total City IDs fetched: {len(city_id_list)}")

    initialize_tracking_file(tracking_file_path, city_id_list)
    remaining_ids = read_tracking_file(tracking_file_path)
    print(f"Remaining City IDs to process: {len(remaining_ids)}")

    while remaining_ids:
        try:
            city_id = random.choice(list(remaining_ids))

            # API credentials
            URL = os.getenv("WEBBADS_URL")
            USERNAME = os.getenv("WEBBADS_USERNAME")
            PASSWORD = os.getenv("WEBBADS_PASSWORD")
            USER_ID = os.getenv("WEBBADS_ID")

            headers = {'Content-Type': 'application/xml'}

            # Fetch hotel data for the city_id
            dotw_search = DOTWHotelSearch(url=URL, username=USERNAME, password=PASSWORD, user_id=USER_ID, headers=headers)
            root = dotw_search.fetch_hotel_data(city_code=city_id)

            if root is not None:
                try:
                    hotels = dotw_search.parse_hotel_data(root)
                    if hotels:
                        for hotel in hotels:
                            hotel_id = hotel.get("hotel_id")
                            if hotel_id:
                                # Save each hotel as a JSON file
                                file_path = os.path.join(folder_path, f"{hotel_id}.json")
                                with open(file_path, "w", encoding="utf-8") as json_file:
                                    json.dump(hotel, json_file, indent=4)
                                print(f"Saved hotel data: {file_path}")

                        remaining_ids.remove(city_id)
                        write_tracking_file(tracking_file_path, remaining_ids)
                        print(f"Processed and removed City ID: {city_id}")
                    else:
                        print(f"No hotels found for City ID: {city_id}")
                        remaining_ids.remove(city_id)
                        write_tracking_file(tracking_file_path, remaining_ids)
                        print(f"Processed and removed City ID: {city_id}")
                        continue
                except Exception as e:
                    print(f"Error processing hotels for City ID {city_id}: {e}")
                    remaining_ids.remove(city_id)
                    write_tracking_file(tracking_file_path, remaining_ids)
                    print(f"Processed and removed City ID: {city_id}")
                    continue
            else:
                print(f"Failed to fetch data for City ID: {city_id}")
                remaining_ids.remove(city_id)
                write_tracking_file(tracking_file_path, remaining_ids)
                print(f"Processed and removed City ID: {city_id}")
                continue
        except Exception as e:
            print(f"Unexpected error during processing: {e}")
            remaining_ids.remove(city_id)
            write_tracking_file(tracking_file_path, remaining_ids)
            print(f"Processed and removed ------------------------- City ID: {city_id}")
            continue


if __name__ == "__main__":
    folder_path = 'D:/content_for_hotel_json/HotelInfo/WebBeds'
    tracking_file_path = 'tracking_file_for_webbeds_content_create.txt'
    save_json_files_follow_systemId(folder_path, tracking_file_path)


















