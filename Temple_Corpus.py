import requests
from bs4 import BeautifulSoup
import re
import urllib3
from urllib.parse import urljoin
from urllib.error import URLError
from urllib.robotparser import RobotFileParser

import mysql.connector
from geopy.geocoders import Nominatim

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'temp_test'
}

# Connection and cursor settings
connection = mysql.connector.connect(**db_config)

cursor = connection.cursor()
# end of connection settings

my_list_web = []
my_list = []
list_p = []
list_img = []
json_data1 = []
i = 0


# ________________________________________________________________________________________________________________________________
# Insertion code for the db connection

# Check for phone number
def update_temple_phone(temple_name, phone_official_site):
    phone_number_pattern = r'^\d{4}\s\d{3}\s\d{4}$|^\d{11}$'
    check_placeholder_sql = "SELECT phone_official_site FROM temples WHERE temple_name = %s"
    cursor.execute(check_placeholder_sql, (temple_name,))
    existing_phone = cursor.fetchone()[0]
    existing_phone = existing_phone.strip()

    if existing_phone in ["Empty Phone", "Phone number not available ,International phone number not available", ""]:
        if existing_phone != phone_official_site:
            # Update phone number
            update_phone_sql = "UPDATE temples SET phone_official_site = %s WHERE temple_name = %s"
            cursor.execute(update_phone_sql, (phone_official_site, temple_name))
            connection.commit()
            print(f"Phone number updated for {temple_name}.")

    elif existing_phone and re.match(phone_number_pattern, existing_phone):
        if existing_phone != phone_official_site:
            # Update the database with the new combined numbers
            update_phone_sql = "UPDATE temples SET phone_official_site = %s WHERE temple_name = %s"
            cursor.execute(update_phone_sql, (phone_official_site, temple_name))
            connection.commit()
            print(f"Phone number updated for {temple_name}.")
    else:
        print(f"Skipping update for Phone {temple_name}.")


# Insertion Descriptions to temple_descriptions table
def add_temple_description(temple_name, description, websites):
    fetch_temple_id_sql = "SELECT temple_id FROM temples WHERE temple_name = %s"
    cursor.execute(fetch_temple_id_sql, (temple_name,))
    temple_id = cursor.fetchone()

    if temple_id:
        temple_id = temple_id[0]  # Extracting the temple_id value

        # Inserting or updating temple description with fetched temple_id, description, and websites
        insert_or_update_desc_sql = """
        INSERT INTO temple_descriptions (temple_id, description, websites) 
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        description = VALUES(description), 
        websites = VALUES(websites)
        """
        desc_values = (temple_id, description, websites)
        cursor.execute(insert_or_update_desc_sql, desc_values)
        connection.commit()
        print("Success: Inserted or Updated Description and Website URL")
    else:
        print("Temple not found in the temples table")


def insert_temple_data(temple_name, deity_name, description, image_url, location, latitude, longitude, opening_hours,
                       related_festival, ways_to_book, websites, phone_official_site, email_official_site):
    # Check if the record already exists
    check_sql = "SELECT COUNT(*) FROM temples WHERE temple_name = %s"
    cursor.execute(check_sql, (temple_name,))
    count = cursor.fetchone()[0]

    if count == 0:
        # Record does not exist, perform the insertion
        sql = """
        INSERT INTO temples (
            temple_name, deity_name, description, image_url, location, latitude, longitude, OpeningHours,
            related_festival, ways_to_book, websites, phone_official_site, email_official_site
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            temple_name, deity_name, description, image_url, location, latitude, longitude, opening_hours,
            related_festival, ways_to_book, websites, phone_official_site, email_official_site
        )
        cursor.execute(sql, values)
        connection.commit()
        print("Success Inserted Data")

    # Edit by Aman
    # Assuming you've checked for existence as in your previous code
    elif count > 0:
        # For Updating Phone Number
        update_temple_phone(temple_name, phone_official_site)
        
        # For Add Description to Temple_Descriptions table
        add_temple_description(temple_name, description, websites)

    else:
        print(f"Record for {temple_name} already exists. Skipping insertion.")


# ________________________________________________________________________________________________________________________________

# Location getter functions
# Yet to be implemented? I'm using my API_KEY!


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
                        close_time = period['close']['time']

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


def is_crawling_allowed(url):
    try:
        rp = RobotFileParser()
        robots_url = f"{url.rstrip('/')}/robots.txt"
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch("*", url)
    except URLError as e:
        print(f"Error while fetching robots.txt(robots.txt file does not exist for this website)")
        return True


def format_phone_number(phone_numbers):
    numbers = re.findall(r'\+?[\d\s-]+', phone_numbers)  # Find all numbers (with or without + sign) in the string
    formatted_numbers = []

    for number in numbers:
        cleaned_number = re.sub(r'[-\s]', '', number)  # Remove hyphens and spaces

        country_code = re.match(r'^(\+\d+)', cleaned_number)
        if country_code:
            country_code = country_code.group(1)
            cleaned_number = cleaned_number[len(country_code):]
        else:
            country_code = ''

        formatted_number = country_code + cleaned_number
        formatted_numbers.append(formatted_number)

    return ', '.join(formatted_numbers)  # Join the numbers into a single string


def crawl(url, limit, li, deity_name, address, festival, temple_name):
    flag = 0
    if limit <= 0:
        return
    if not is_crawling_allowed(url):
        print(f"Crawling not allowed for {url}")
        return

    # Send a GET request to the specified URL
    #For SSL certi
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(url, verify=False)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract relevant information or perform desired actions
    # For example, you could print the page title
    # print("Title:", soup.title.string)

    # Extract and print all the text content within the <p> tags
    paragraphs = soup.find_all('p')
    list_p.clear()  # Clear the list don't clear if you want to add older description
    for p in paragraphs:
        cleaned_text = p.text.strip()
        if not cleaned_text or cleaned_text.isspace():
            continue
        pattern = r'[\t]{2,}|@'
        match = re.search(pattern, cleaned_text)
        if match:
            continue
        else:
            list_p.append(cleaned_text)
            print(cleaned_text)

    # Extract and print the URLs of all the images within the <img> tags
    images = soup.find_all('img')
    for image in images:
        src = image.get('src')
        if src not in list_img:
            list_img.append(src)
            print("Image URL: ", src)

    # Find all the links on the page
    links = soup.find_all('a')

    # Process each link
    for link in links:
        href = link.get('href')

        # Check if the link is not None
        if href is not None:
            # Construct the absolute URL
            if href.startswith('https://'):
                if href not in my_list:
                    my_list.append(href)
                    crawl(href, limit - 1, li, deity_name,
                          address, festival, temple_name)
                    if flag % 6 == 0:
                        print("Reference Links:", href)
                    flag = flag + 1

    # Customized code by Ashok
    # Data to MySQL
    temple_name = temple_name
    # deity_name
    description = "\n".join(list_p)
    image_url = list_img[5]
    locationResults = get_location_details(temple_name + ' ' + address)
    location = locationResults['formatted_address'] if locationResults and locationResults[
        'formatted_address'] else "Empty address"
    latitude = locationResults['latitude'] if locationResults and locationResults['latitude'] else "Empty Latitude"
    longitude = locationResults['longitude'] if locationResults and locationResults['longitude'] else "Empty Longitude"
    opening_hours = locationResults['opening_hours'] if locationResults and locationResults[
        'opening_hours'] else "Empty Hours"
    phone_official_site = locationResults['phone_number'] if locationResults and locationResults[
        'phone_number'] else "Empty Phone"

    # Edit by Aman
    phone_official_site = phone_official_site.strip()
    if phone_official_site in ["Empty Phone", "Phone number not available ,International phone number not available", ""]:

        # storing only number which is on the top
        phone_number = re.search(r'\+\d{2}-\d{2}-\d{4}-\d{4}|\b\d{11}\b|\b9\d{9}\b', soup.text)
        if phone_number:
            phone_official_site = phone_number.group()
            phone_official_site = format_phone_number(phone_official_site)
    else:
        phone_official_site = format_phone_number(phone_official_site)

    print(f"After Soup: {phone_official_site}")
    email_official_site = locationResults['email'] if locationResults and locationResults['email'] else "Empty Email"
    related_festival = festival
    ways_to_book = 'To Be Implemented'
    websites = li

    insert_temple_data(temple_name, deity_name, description, image_url, location, latitude, longitude, opening_hours,
                       related_festival, ways_to_book, websites, phone_official_site, email_official_site)


# getting google search results link
def get_google_search_links(query):
    url = f"https://www.google.com/search?q={query}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # Adjust the class based on the current structure
        result_divs = soup.find_all('div', class_='tF2Cxc')

        links = []
        for result_div in result_divs:
            link_tag = result_div.find('a')
            if link_tag:
                href_value = link_tag.get('href')
                clean_url = href_value.split('&')[0]
                links.append(clean_url)

        return links
    else:
        print(f"Failed to fetch {url}. Status code: {response.status_code}")
        return []


# Start the crawler by providing a seed URL
q = input("Enter Topic name: ")
deity = input("Enter Deity name: ")
address = input("Enter Address: ")
festival = input("Enter Festival: ")
links = get_google_search_links(q)
for link in links[:3]:
    if link.startswith("ppp"):
        continue
    else:
        result_web = link
        if result_web not in my_list_web:
            my_list_web.append(result_web)
            print("\nWebsite Url: ", link, "\n")
            crawl(link, 1, link, deity, address, festival, q)
