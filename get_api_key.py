from seleniumwire import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests

# Set up Chrome options
# Set up Selenium WebDriver (assuming Chrome)
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run in headless mode
driver = webdriver.Chrome(options=options)


# Open the target URL
target_url = "https://worldathletics.org/competition/calendar-results?"  # Replace with the actual URL
driver.get(target_url)

# Wait for the page to load (adjust the time as needed)
time.sleep(5)

# Function to extract and print headers from network requests
def extract_and_print_headers(driver):
    api_key = None
    for request in driver.requests:
        if request.response:
            print(f"Request URL: {request.url}")
            print("Request Headers:")
            for header, value in request.headers.items():
                print(f"{header}: {value}")
                if header.lower() == 'x-api-key':
                    api_key = value
            print("\nResponse Headers:")
            for header, value in request.response.headers.items():
                print(f"{header}: {value}")
            print("\n" + "-"*50 + "\n")
    return api_key

# Extract the API key from the headers
api_key = extract_and_print_headers(driver)

if api_key:
    print(f"API key found: {api_key}")
else:
    print("API key not found in the headers.")

# Close the WebDriver
driver.quit()