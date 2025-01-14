import requests
import json
from datetime import datetime 


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
                for media in medias:
                    media_code = media.get("code") or "NULL"
                    media_url = media.get("url") or "NULL"
                    hotel_photos.append({
                        "picture_id": media_code,
                        "title": "NULL",
                        "url": media_url
                    })

                primary_photo = hotel_photos[0]["url"] if hotel_photos else "NULL"

                data = {
                    "createdAt": createdAt_str,
                    "timeStamp": timeStamp,
                    "hotel_id": hotel_code,
                    "name": hotel_name,
                    "name_local": hotel_name,
                    "name_formerly_name": hotel_name,
                    # "destination_code": "NULL",
                    # "country_code": hotel_data.get("location", {}).get("country") or "NULL",
                    # "brand_text": hotel_data.get("propertyType", {}).get("name") or "NULL",
                    # "property_type": hotel_data.get("propertyType", {}).get("name") or "NULL",
                    # "star_rating": hotel_data.get("categoryCode") or "NULL",
                    # "chain": hotel_data.get("chainCode") or "NULL",
                    # "brand:": "NULL",
                    # "logo": "NULL",
                    # "primary_photo": primary_photo,
                    # "hotel_photo": hotel_photos,
                    "Number": x
                }
                extracted_data.append(data)
            return extracted_data
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
        

    def display_hotels(self, hotel_list):
        return json.dumps(hotel_list, indent=4)


# Example usage
if __name__ == "__main__":
    api_key = "884eaecb-a32a-44b3-9820-ad5490ec1ee0"
    criteria_hotels = {"access": "29887"}

    travelgatex_api = TravelGateXAPI(api_key)
    
    try:
        response = travelgatex_api.fetch_hotels(criteria_hotels)
        hotel_list = travelgatex_api.extract_hotel_data(response)
        json_response = travelgatex_api.display_hotels(hotel_list)
        print(json_response)
    except Exception as e:
        print(f"Error: {e}")
