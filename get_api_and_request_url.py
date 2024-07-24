from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import time
import json

# Configure Selenium WebDriver for Edge
options = EdgeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')

# Set logging preferences directly in the options
capabilities = DesiredCapabilities.EDGE.copy()
capabilities['ms:loggingPrefs'] = {'performance': 'ALL'}
options.set_capability("ms:loggingPrefs", {'performance': 'ALL'})

# Initialize WebDriver using the most recent WebDriver for Edge
driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()), options=options)

# Open the webpage
url = 'https://worldathletics.org/competition/calendar-results'
driver.get(url)

# Wait for the page to load
time.sleep(10)  # Adjust the sleep time as necessary

# Extract network logs
logs = driver.get_log('performance')

# Filter out the necessary logs
for log in logs:
    log_json = json.loads(log['message'])['message']
    if log_json['method'] == 'Network.requestWillBeSent':
        request = log_json['params']['request']
        if request['method'] == 'POST' and 'graphql' in request['url']:
            headers = request['headers']
            request_url = request['url']
            api_key = headers.get('x-api-key')
            if api_key:
                print(f"Request URL: {request_url}")
                print(f"x-api-key: {api_key}")
                break

# Close the WebDriver
driver.quit()




