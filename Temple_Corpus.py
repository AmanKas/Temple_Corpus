import requests
from bs4 import BeautifulSoup
import re
import urllib3

from location_details_api import get_location_details
from urllib.error import URLError
from urllib.robotparser import RobotFileParser
from urllib.parse import unquote

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
    # Phone Regex
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
            print(f"Success: Phone number updated for {temple_name}.")

    elif existing_phone and re.match(phone_number_pattern, existing_phone):
        if existing_phone != phone_official_site:
            # Update the database with the new combined numbers
            update_phone_sql = "UPDATE temples SET phone_official_site = %s WHERE temple_name = %s"
            cursor.execute(update_phone_sql, (phone_official_site, temple_name))
            connection.commit()
            print(f"Success: Phone number updated for {temple_name}.")
    # else:
    #     print(f"Skipping update for Phone {temple_name}.")


# Insertion Descriptions to temple_descriptions table
def add_temple_description(temple_name, description, websites):
    cursor = connection.cursor(buffered=True)
    fetch_temple_id_sql = "SELECT temple_id FROM temples WHERE temple_name = %s"
    cursor.execute(fetch_temple_id_sql, (temple_name,))
    temple_id = cursor.fetchone()

    if temple_id:
        temple_id = temple_id[0]  # Extracting the temple_id value

        # Fetch existing website
        fetch_existing_websites_sql = "SELECT websites FROM temple_descriptions WHERE temple_id = %s"
        cursor.execute(fetch_existing_websites_sql, (temple_id,))
        existing_websites = cursor.fetchall()

        # Fetch from temples table
        fetch_website_sql = "SELECT websites FROM temples WHERE temple_name = %s"
        cursor.execute(fetch_website_sql, (temple_name,))
        result = cursor.fetchone()
        existing_websites.append(result)

        # Flatten list of tuples into list of strings
        existing_websites = [website[0] for website in existing_websites]

        # Check if new website is in existing websites
        if websites not in existing_websites:
            # Inserting or updating temple description with fetched temple_id, description, and new_website
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
        # else:
        #     print("Website URL is already in the database. No update needed.")


# Update Address
def update_temple_address(temple_name, new_location):
    # Fetch existing address
    fetch_existing_address_sql = "SELECT location FROM temples WHERE temple_name = %s"
    cursor.execute(fetch_existing_address_sql, (temple_name,))
    existing_location = cursor.fetchone()

    # Check if new address is longer than existing address
    if existing_location and len(new_location) > len(existing_location[0]):
        # Update address
        update_address_sql = """
        UPDATE temples
        SET location = %s
        WHERE temple_name = %s
        """
        cursor.execute(update_address_sql, (new_location, temple_name))
        connection.commit()
        print("Success: Updated Address")
    # else:
    #     print("Address is already in the database. No update needed.")


def update_temple_email(temple_name, new_email):
    # Fetch existing email
    fetch_existing_email_sql = "SELECT email_official_site FROM temples WHERE temple_name = %s"
    cursor.execute(fetch_existing_email_sql, (temple_name,))
    existing_email = cursor.fetchone()

    # Check if new email is not empty and different from existing email
    if existing_email and new_email and new_email != existing_email[0] and new_email != "Email not available":
        # Update email
        update_email_sql = """
        UPDATE temples
        SET email_official_site = %s
        WHERE temple_name = %s
        """
        cursor.execute(update_email_sql, (new_email, temple_name))
        connection.commit()
        print("Success: Updated Email")
    # else:
    #     print("Email is already in the database. No update needed.")


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
        print("Success: Inserted Data")

    # Edit by Aman
    # Assuming you've checked for existence as in your previous code
    elif count > 0:
        # For Updating Phone Number
        update_temple_phone(temple_name, phone_official_site)

        # For Add Description to Temple_Descriptions table
        add_temple_description(temple_name, description, websites)

        # For Updating Address
        update_temple_address(temple_name, location)

        # For Updating Email
        update_temple_email(temple_name, email_official_site)

    else:
        print(f"Multiple Record for {temple_name} already exists. Skipping insertion.")


def insert_temple_data_by_api(temple_name, location, latitude, longitude, opening_hours, phone_official_site,
                              email_official_site):
    # SQL query to fetch the temple_id
    fetch_temple_id_sql = "SELECT temple_id FROM temples WHERE temple_name = %s"
    cursor.execute(fetch_temple_id_sql, (temple_name,))
    result = cursor.fetchone()

    # Extract the temple_id from the tuple
    temple_id = result[0] if result else None

    # SQL query to update the temple details
    update_sql = """
    UPDATE temples
    SET  location = %s, latitude = %s, longitude = %s, OpeningHours = %s
    WHERE temple_id = %s
    """
    values = (location, latitude, longitude, opening_hours, temple_id)

    # Execute the SQL query
    cursor.execute(update_sql, values)
    connection.commit()

    # For Updating Phone Number
    update_temple_phone(temple_name, phone_official_site)
    # For Updating Email
    update_temple_email(temple_name, email_official_site)
    print("Success :Updated with API")


def fetch_temple_address(temple_name):
    # SQL query to fetch the address
    fetch_address_sql = "SELECT location FROM temples WHERE temple_name = %s"
    cursor.execute(fetch_address_sql, (temple_name,))
    result = cursor.fetchone()
    # Extract the address from the tuple
    address = result[0] if result else None

    return address


# ________________________________________________________________________________________________________________________________

# Formating Phone Number
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


def crawl(url, limit, li, deity_name, address, festival, temple_name):
    flag = 0
    if limit <= 0:
        return

    # Send a GET request to the specified URL
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

    # Declaring Variables
    phone_official_site = "Phone number not available ,International phone number not available"
    location = address
    longitude = 0.0
    latitude = 0.0
    opening_hours = "Not Found Timing"
    email_official_site = "Email not available"

    # Scraped Description
    description = "\n".join(list_p)

    # Scraped Images Url
    image_url = list_img[5]

    # Scraping Phone Number
    # Designed to only store the first number
    phone_number = re.search(r'\+\d{2}-\d{2}-\d{4}-\d{4}|\b\d{11}\b|\b9\d{9}\b|\+91-\d{10}', soup.text)
    if phone_number:
        phone_official_site = phone_number.group()
        phone_official_site = format_phone_number(phone_official_site)

    email_tags = soup.find_all('a', href=True)
    scrap_email = ' '.join(
        [re.sub(r'^mailto:', '', unquote(tag['href'])).strip() for tag in email_tags if 'mailto:' in tag['href']])
    if re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', scrap_email):
        email_official_site = scrap_email

    # Scraping Address
    address_h3 = soup.find('h3', string='Contact Details')
    if address_h3:
        address_paragraph = address_h3.find_next('p')
        if address_paragraph:
            address = address_paragraph.get_text(separator='\n')
            location = address
        # else:
        #     print("Address details not found")
    # else:
    #     print("Address header not found")

    # To be updated
    related_festival = festival
    ways_to_book = 'To Be Implemented'
    websites = li
    # Insert Data into Database
    insert_temple_data(temple_name, deity_name, description, image_url, location, latitude, longitude, opening_hours, related_festival, ways_to_book, websites, phone_official_site, email_official_site)


def get_temple_details_by_api(temple_name):
    # fetching address from the temples tables
    fetch_address = fetch_temple_address(temple_name)
    address = fetch_address.split()[0] if fetch_address else None
    locationResults = get_location_details(temple_name + ' ' + address)
    location = locationResults['formatted_address'] if locationResults and locationResults[
        'formatted_address'] else "Empty address"
    latitude = locationResults['latitude'] if locationResults and locationResults['latitude'] else "Empty Latitude"
    longitude = locationResults['longitude'] if locationResults and locationResults['longitude'] else "Empty Longitude"
    opening_hours = locationResults['opening_hours'] if locationResults and locationResults[
        'opening_hours'] else "Empty Hours"
    phone_official_site = locationResults['phone_number'] if locationResults and locationResults[
        'phone_number'] else "Empty Phone"
    email_official_site = locationResults['email'] if locationResults and locationResults['email'] else "Empty Email"
    insert_temple_data_by_api(temple_name, location, latitude, longitude, opening_hours, phone_official_site,
                              email_official_site)


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
        print(f"Error: Failed to fetch {url}. Status code: {response.status_code}")
        return []


# Start the crawler by providing a seed URL
q = input("Enter Topic name: ")
deity = input("Enter Deity name: ")
address = input("Enter Address: ")
festival = input("Enter Festival: ")
links = get_google_search_links(q)
crawlable_links_count = 0
index = 0
while crawlable_links_count < 3 and index < len(links):
    link = links[index]
    if link.startswith("ppp"):
        index += 1
        continue
    else:
        result_web = link
        if result_web not in my_list_web:
            my_list_web.append(result_web)
            print("\nWebsite Url: ", link, "\n")
            if is_crawling_allowed(link):
                crawl(link, 1, link, deity, address, festival, q)
                crawlable_links_count += 1
            else:
                print(f"Warning: Crawling not allowed for {link} skipping this link")
    index += 1
# API Call and Update
get_temple_details_by_api(q)
