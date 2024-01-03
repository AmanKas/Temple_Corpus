import requests


def get_location_details(address):
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    places_base_url = "https://maps.googleapis.com/maps/api/place/details/json"
    api_key = "AIzaSyDVVrTpVbd6OBebEdy6kE_V0RyQIPqDBhI"
    # Step 1: Get coordinates (latitude and longitude) using Geocoding API
    params = {'address': address, 'key': api_key}
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()

        # Check if the Geocoding API request was successful
        if data.get('status') == 'OK':
            result = data['results'][0]

            # Extract latitude, longitude, and address from the results
            location = result['geometry']['location']
            latitude = location['lat']
            longitude = location['lng']
            formatted_address = result['formatted_address']

            # Step 2: Get place details using Places API
            places_params = {
                'place_id': result['place_id'],
                'key': api_key,
                'fields': 'name,formatted_phone_number,international_phone_number,website,opening_hours'
            }

            # Send a GET request to the Google Places API
            places_response = requests.get(
                places_base_url, params=places_params)
            places_data = places_response.json()

            # Check if the Places API request was successful
            if places_response.status_code == 200 and places_data.get('status') == 'OK':
                result = places_data.get('result', {})
                phone_number = result.get(
                    'formatted_phone_number', 'Phone number not available')
                international_phone_number = result.get(
                    'international_phone_number', 'International phone number not available')
                website = result.get('website', 'Website not available')
                email = result.get('email', 'Email not available')
                opening_hours_info = result.get('opening_hours', {})
                if opening_hours_info and 'periods' in opening_hours_info:
                    # Extract opening hours from periods if available
                    periods = opening_hours_info['periods']
                    opening_hours = []

                    for period in periods:
                        day = period['open']['day']
                        open_time = period['open']['time']
                        close_time = period['close']['time'] if 'close' in period else '00:00'

                        day_name = [
                            'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
                        ][day]

                        hours = f"{day_name}\n{open_time}–{close_time}"

                        opening_hours.append(hours)

                    opening_hours = '\n\n'.join(opening_hours)
                else:
                    opening_hours = 'Opening hours not available'

                return {
                    'formatted_address': formatted_address,
                    'latitude': latitude,
                    'longitude': longitude,
                    'opening_hours': opening_hours,
                    'phone_number': phone_number + ' ,' + international_phone_number,
                    'website': website,
                    'email': email
                }
            else:
                print("Error: Unable to fetch details from Google Places API.")
    else:
        print(
            f"Error: Unable to fetch data from Geocoding API. Status Code: {response.status_code}")

    return None
