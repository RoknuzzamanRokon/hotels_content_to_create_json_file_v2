import requests
import json
from datetime import datetime 
import os


class TravelGateXAPI:
    def __init__(self, api_key, url="https://api.travelgatex.com"):
        self.api_key = api_key
        self.url = url
        self.headers = {
            'Authorization': f'Apikey {self.api_key}',
            'Accept': 'gzip',
            'Connection': 'keep-alive',
            'TGX-Content-Type': 'graphqlx/json',
            'Content-Type': 'application/json'
        }

    def fetch_hotels(self, criteria_hotels, language="en", token=""):
        payload = {
                    "query": (
                        "query ($criteriaHotels: HotelXHotelListInput!, $language: [Language!], $token: String) {"
                        "  hotelX {"
                        "    hotels(criteria: $criteriaHotels, token: $token) {"
                        "      token"
                        "      count"
                        "      edges {"
                        "        node {"
                        "          createdAt"
                        "          updatedAt"
                        "          hotelData {"
                        "            hotelCode"
                        "            hotelName"
                        "            categoryCode"
                        "            chainCode"
                        "            mandatoryFees {"
                        "              duration"
                        "              scope"
                        "              name"
                        "              text"
                        "              price {"
                        "                amount"
                        "                currency"
                        "              }"
                        "            }"
                        "            giataData {"
                        "              id"
                        "            }"
                        "            checkIn {"
                        "              schedule {"
                        "                startTime"
                        "              }"
                        "              minAge"
                        "              instructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "              specialInstructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "            }"
                        "            checkOut {"
                        "              schedule {"
                        "                startTime"
                        "              }"
                        "              minAge"
                        "              instructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "              specialInstructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "            }"
                        "            location {"
                        "              address"
                        "              zipCode"
                        "              city"
                        "              country"
                        "              coordinates {"
                        "                latitude"
                        "                longitude"
                        "              }"
                        "              closestDestination {"
                        "                code"
                        "                available"
                        "                texts(languages: $language) {"
                        "                  text"
                        "                  language"
                        "                }"
                        "                type"
                        "                parent"
                        "              }"
                        "            }"
                        "            contact {"
                        "              email"
                        "              telephone"
                        "              fax"
                        "              web"
                        "            }"
                        "            propertyType {"
                        "              propertyCode"
                        "              name"
                        "            }"
                        "            descriptions(languages: $language) {"
                        "              type"
                        "              texts {"
                        "                language"
                        "                text"
                        "              }"
                        "            }"
                        "            medias {"
                        "              code"
                        "              url"
                        "            }"
                        "            allAmenities {"
                        "              edges {"
                        "                node {"
                        "                  amenityData {"
                        "                    code"
                        "                    amenityCode"
                        "                  }"
                        "                }"
                        "              }"
                        "            }"
                        "          }"
                        "        }"
                        "      }"
                        "    }"
                        "  }"
                        "}"
                    ),
            "variables": {
                "criteriaHotels": criteria_hotels,
                "language": language,
                "token": token
            }
        }
        response = requests.post(self.url, headers=self.headers, json=payload)
        return response

    def extract_hotel_data(self, response):
        if response.status_code == 200:
            data = response.json()
            hotels = data.get("data", {}).get("hotelX", {}).get("hotels", {}).get("edges", [])
            extracted_data = []
            x = 0
            for hotel in hotels:
                x += 1

                createdAt = datetime.now()
                createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
                created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
                timeStamp = int(created_at_dt.timestamp())
                
                hotel_data = hotel.get("node", {}).get("hotelData", {})

                hotel_code = hotel_data.get("hotelCode") or "NULL"
                hotel_name = hotel_data.get("hotelName") or "NULL"

                # Find hotel photos. 
                hotel_photos = []
                medias = hotel_data.get("medias", [])
                if medias is not None:
                    for media in medias:
                        media_code = media.get("code") or "NULL"
                        media_url = media.get("url") or "NULL"
                        hotel_photos.append({
                            "picture_id": media_code,
                            "title": "NULL",
                            "url": media_url
                        })

                primary_photo = hotel_photos[0]["url"] if hotel_photos else "NULL"

                
                # Genarate data for google links.
                address_line_1 = hotel_data.get("location", {}).get("address") or "NULL",
                hotel_name = hotel_data.get("hotelName") or "NULL"
                address_query = f"{address_line_1}, {hotel_name}"
                google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None


                # Description data formating.
                description = hotel_data.get("descriptions", {}) or "NULL"
                description_info = []
                for desc in description:
                    desc_type = desc.get("type", "NULL")
                    texts = desc.get("texts", [])
                    for text_entry in texts:
                        text = text_entry.get("text", "NULL")
                        description_info.append({
                            "title": desc_type,
                            "text": text
                        })


                # Assuming hotel_data is your input data
                all_amenities = hotel_data.get("allAmenities", {}).get("edges", [])

                # Extract amenities into the desired format
                amenities_list = [
                    {
                        "type": amenity.get("node", {}).get("amenityData", {}).get("amenityCode", "NULL"),
                        "title": "NULL",
                        "icon": "mdi mdi-alpha-f-circle-outline"
                    }
                    for amenity in all_amenities
                ]

                data = {
                    "createdAt": createdAt_str,
                    "timeStamp": timeStamp,
                    "hotel_id": hotel_code,
                    "name": hotel_name,
                    "name_local": hotel_name,
                    "name_formerly_name": hotel_name,
                    "destination_code": "NULL",
                    "country_code": hotel_data.get("location", {}).get("country") or "NULL",
                    "brand_text": "NULL",
                    "property_type": hotel_data.get("propertyType", {}) or "NULL",
                    "star_rating": hotel_data.get("categoryCode") or "NULL",
                    "chain": hotel_data.get("chainCode") or "NULL",
                    "brand:": "NULL",
                    "logo": "NULL",
                    "primary_photo": primary_photo,
                    "review_rating": {
                        "source": "N/A",
                        "number_of_reviews": "N/A",
                        "rating_average": "N/A",
                        "popularity": "N/A"
                        },
                        
                    "policies": {
                        "checkin": {
                            "begin_time": hotel_data.get("checkIn") or "NULL",
                            "end_time": hotel_data.get("checkOut") or "NULL",
                            "instructions": None,
                            "special_instructions": None,
                            "min_age":  None,
                            },
                        "checkout": {
                            "time": hotel_data.get("checkOut") or "NULL",
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
                        "latitude": hotel_data.get("location", {}).get("coordinates", {}).get("latitude") or "NULL",
                        "longitude": hotel_data.get("location", {}).get("coordinates", {}).get("longitude") or "NULL",
                        "address_line_1": hotel_data.get("location", {}).get("address") or "NULL",
                        "address_line_2": None,
                        "city": hotel_data.get("location", {}).get("city") or "NULL",
                        "state": None,
                        "country": None,
                        "country_code": hotel_data.get("location", {}).get("country") or "NULL",
                        "postal_code": hotel_data.get("location", {}).get("zipCode") or "NULL",
                        "full_address": hotel_data.get("location", {}).get("address") or "NULL",
                        "google_map_link": google_map_site_link,
                        "local_lang": {
                            "latitude": hotel_data.get("location", {}).get("coordinates", {}).get("latitude") or "NULL",
                            "longitude": hotel_data.get("location", {}).get("coordinates", {}).get("longitude") or "NULL",
                            "address_line_1": hotel_data.get("location", {}).get("address") or "NULL",
                            "address_line_2": None,
                            "city": hotel_data.get("location", {}).get("city") or "NULL",
                            "state": None,
                            "country": None,
                            "country_code": hotel_data.get("location", {}).get("country") or "NULL",
                            "postal_code": hotel_data.get("location", {}).get("zipCode") or "NULL",
                            "full_address": hotel_data.get("location", {}).get("address") or "NULL",
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
                    
                    "contact": {
                            "email": hotel_data.get("contact", {}).get("email") or "NULL",
                            "phone": hotel_data.get("contact", {}).get("telephone") or "NULL",
                            "fax": hotel_data.get("contact", {}).get("fax") or "NULL",
                            "website": hotel_data.get("contact", {}).get("web") or "NULL",
                            },
                    "description": description_info,
                    "room_type": None,
                    "sponken_language": None,
                    "amenities": amenities_list,
                    "facilities": None,
                    "hotel_photo": hotel_photos,
                    "point_of_interests": None,
                    "nearest_airports": None,
                    "train_stations": None,
                    "connected_locations": None,
                    "stadiums": None
                }
                extracted_data.append(data)
            return extracted_data
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

    def display_hotels(self, hotel_list):
        return json.dumps(hotel_list, indent=4)
    


    def fetch_and_save_hotels(self, criteria_hotels, folder_path):
        token = ""
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        while True:
            try:
                response = self.fetch_hotels(criteria_hotels, token=token)
                if response.status_code != 200:
                    raise Exception(f"Failed to fetch hotels: {response.status_code}, {response.text}")

                data = response.json()
                hotels = data.get("data", {}).get("hotelX", {}).get("hotels", {})
                token = hotels.get("token", "")
                edges = hotels.get("edges", [])

                for edge in edges:
                    try:
                        hotel_data = self.extract_hotel_data(edge["node"])
                        hotel_id = hotel_data["hotel_id"]
                        file_path = os.path.join(folder_path, f"{hotel_id}.json")

                        if os.path.exists(file_path):
                            print(f"Skipping existing hotel_id: {hotel_id}")
                            continue

                        with open(file_path, "w", encoding="utf-8") as f:
                            json.dump(hotel_data, f, indent=4)
                            print(f"Saved hotel_id: {hotel_id}")
                    except Exception as e:
                        print(f"Error processing hotel data: {e}")

                if not token:
                    break
            except Exception as e:
                print(f"Error fetching data: {e}")



# Example usage
if __name__ == "__main__":
    api_key = "884eaecb-a32a-44b3-9820-ad5490ec1ee0"
    criteria_hotels = {"access": "30336", "maxSize": 5}
    output_folder = "D:/content_for_hotel_json/HotelInfo/illusionsonline"

    travelgatex_api = TravelGateXAPI(api_key)
    travelgatex_api.fetch_and_save_hotels(criteria_hotels, output_folder)



