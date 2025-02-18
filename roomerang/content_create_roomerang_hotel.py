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
        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code} - {response.text}")
        return response


    def extract_hotel_data(self, response):
        try:
            if response.status_code == 200:
                data = response.json()
                get_token = data.get("data", {}).get("hotelX", {}).get("hotels", {}).get("token", {})
                # print(get_token)
                
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
                    
                    if hotel_data is None:  
                        hotel_id = hotel.get("node", {}).get("hotelCode", "UNKNOWN")
                        print(f"Error with hotel_id: {hotel_id}") 
                        continue  
                    
                    hotel_code = hotel_data.get("hotelCode") or "NULL"
                    hotel_name = hotel_data.get("hotelName") or "NULL"

                    # Find hotel photos
                    hotel_photos = []

                    medias = hotel_data.get("medias", [])
                    if isinstance(medias, list):  
                        for media in medias:
                            if isinstance(media, dict): 
                                media_code = media.get("code", "NULL")  
                                media_url = media.get("url", "NULL")   
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




                    description = hotel_data.get("descriptions", [])
                    if not isinstance(description, (list, dict)):
                        description = []  
                    description_info = []

                    for desc in description:
                        if not isinstance(desc, dict):
                            continue

                        desc_type = desc.get("type", "NULL")
                        texts = desc.get("texts", [])

                        if not isinstance(texts, list):
                            continue

                        for text_entry in texts:
                            if not isinstance(text_entry, dict):
                                continue

                            text = text_entry.get("text", "NULL")
                            description_info.append({
                                "title": desc_type,
                                "text": text
                            })

                    # all_amenities = hotel_data.get("allAmenities", {}).get("edges", [])

                    # if isinstance(all_amenities, list):
                    #     amenities_list = [
                    #         {
                    #             "type": amenity.get("node", {}).get("amenityData", {}).get("amenityCode", "NULL"),
                    #             "title": "NULL",  
                    #             "icon": "mdi mdi-alpha-f-circle-outline"
                    #         }
                    #         for amenity in all_amenities if isinstance(amenity, dict)
                    #     ]
                    #     amenities_list = amenities_list if amenities_list else "NULL"
                    # else:
                    #     amenities_list = "NULL"

                    data = {
                        "created": createdAt_str,
                        "timestamp": timeStamp,
                        "hotel_id": hotel_code,
                        "name": hotel_name,
                        "name_local": hotel_name,
                        "hotel_formerly_name": hotel_name,
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
                            "popularity_score": "N/A"
                            },
                            
                        "policies": {
                            "checkin": {
                                "begin_time": hotel_data.get("checkIn") or "NULL",
                                "end_time": hotel_data.get("checkOut") or "NULL",
                                "instructions": "NULL",
                                "min_age":  "NULL",
                                },
                            "checkout": {
                                "time": hotel_data.get("checkOut") or "NULL",
                                },
                            "fees": {
                                "optional": "NULL",
                                "mandatory": "NULL",
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
                            "latitude": hotel_data.get("location", {}).get("coordinates", {}).get("latitude") or "NULL",
                            "longitude": hotel_data.get("location", {}).get("coordinates", {}).get("longitude") or "NULL",
                            "address_line_1": hotel_data.get("location", {}).get("address") or "NULL",
                            "address_line_2": "NULL",
                            "city": hotel_data.get("location", {}).get("city") or "NULL",
                            "state": "NULL",
                            "country": "NULL",
                            "country_code": hotel_data.get("location", {}).get("country") or "NULL",
                            "postal_code": hotel_data.get("location", {}).get("zipCode") or "NULL",
                            "full_address": hotel_data.get("location", {}).get("address") or "NULL",
                            "google_map_site_link": google_map_site_link,
                            "local_lang": {
                                "latitude": hotel_data.get("location", {}).get("coordinates", {}).get("latitude") or "NULL",
                                "longitude": hotel_data.get("location", {}).get("coordinates", {}).get("longitude") or "NULL",
                                "address_line_1": hotel_data.get("location", {}).get("address") or "NULL",
                                "address_line_2": "NULL",
                                "city": hotel_data.get("location", {}).get("city") or "NULL",
                                "state": "NULL",
                                "country": "NULL",
                                "country_code": hotel_data.get("location", {}).get("country") or "NULL",
                                "postal_code": hotel_data.get("location", {}).get("zipCode") or "NULL",
                                "full_address": hotel_data.get("location", {}).get("address") or "NULL",
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
                                "phone_numbers": [hotel_data.get("contact", {}).get("telephone") or "NULL"],
                                "email_address": [hotel_data.get("contact", {}).get("email") or "NULL"],
                                "fax": [hotel_data.get("contact", {}).get("fax") or "NULL"],
                                "website": [hotel_data.get("contact", {}).get("web") or "NULL"],
                                },
                        "description": description_info,
                        "room_type": "NULL",
                        "sponken_language": "NULL",
                        "amenities": "NULL",
                        "facilities": "NULL",
                        "hotel_photo": hotel_photos,
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
                    extracted_data.append(data)
                return extracted_data, get_token

        except Exception as e:
            print(f"An error occurred while extracting hotel data: {str(e)}")



    def display_hotels(self, hotel_list):
        return json.dumps(hotel_list, indent=4)
    

def fetch_and_save_hotels_in_json(criteria_hotels, output_dir):
    api_key = "884eaecb-a32a-44b3-9820-ad5490ec1ee0"
    travelgatex_api = TravelGateXAPI(api_key)

    token = ""
    while token is not None:
        try:
            response = travelgatex_api.fetch_hotels(criteria_hotels, token=token)
            hotel_list, token = travelgatex_api.extract_hotel_data(response)

            # Process and save the hotels
            for hotel_data in hotel_list:
                try:
                    hotel_id = hotel_data.get("hotel_id", "unknown")  
                    output_file = os.path.join(output_dir, f"{hotel_id}.json")

                    # Skip if the file for this hotel ID already exists
                    if os.path.exists(output_file):
                        print(f"Skipping........................................................ {hotel_id}, file already exists.")
                        continue

                    # Save hotel data to JSON
                    os.makedirs(output_dir, exist_ok=True)
                    with open(output_file, 'w') as file:
                        json.dump(hotel_data, file, indent=4)
                    print(f"Hotel data saved to {output_file}")
                except Exception as e:
                    print(f"Error saving hotel data: {e}")
                    continue  
        except Exception as e:
            print(f"Error fetching hotels: {e}")
            continue 


# Example usage
if __name__ == "__main__":
    criteria_hotels = {"access": "30846"}
    output_folder = "D:/content_for_hotel_json/HotelInfo/roomerang"
    fetch_and_save_hotels_in_json(criteria_hotels, output_folder)