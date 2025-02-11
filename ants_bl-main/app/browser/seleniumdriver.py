import os
import random
import re
import time

import undetected_chromedriver as uc

from app.proxy.proxy_auth import manifest_json, background_js
from app.database.db import DatabaseManager
from config.settings import DATABASE_URL_TEST


class SeleniumWebDriver:

    def __init__(self, use_proxy: bool = True, headless: bool = True, data: DatabaseManager = None):
        self.use_proxy = use_proxy
        self.headless = headless
        self.load_timeout = 40  # seconds
        self.data = data or DatabaseManager(DATABASE_URL_TEST)

    def check_ip(self, driver):
        httpbin = 'https://httpbin.org/ip'
        ipinfo = 'https://ipinfo.io/json'

        if not hasattr(driver, 'was_verified'):
            try:
                print("Checking IP")
                driver.get(httpbin)
                print(driver.page_source)
                assert re.search(r'\"origin\"', driver.page_source)
            except Exception:
                print(f"Failed from {httpbin}")
                driver.get(ipinfo)

        setattr(driver, 'was_verified', True)

    def open_browser(self, proxy=None):
        chrome_options = uc.ChromeOptions()
        chrome_options.headless = self.headless
        if self.headless:
            chrome_options.add_argument("--headless")

        chrome_options.page_load_timeout = self.load_timeout
        chrome_options.set_capability("pageLoadStrategy", "eager")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--aggressive-cache-discard")
        chrome_options.add_argument("--disable-cache")
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disable-offline-load-stale-cache") 
        chrome_options.add_argument("--disk-cache-size=0")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")
        chrome_options.add_argument("--disable-browser-side-navigation")
        chrome_options.add_argument("start-maximized")
        chrome_options.accept_insecure_certs = True


        if self.use_proxy:
            # get random proxy and parse it
            ip_address = proxy["ip_address"]
            provider = proxy["provider"]
            auth = proxy["auth_type"]
            print(f"Using proxy: {ip_address}/{provider}/{auth}")
            if int(auth) == 2: 
                user = proxy["user_proxy"]
                pwd = proxy["pass_proxy"]
            else:
                user = pwd = None

            ip, port = proxy["ip_address"], proxy["port"]

            # Creating folder and files for plugin will support proxy
            plugin_file = os.path.join(os.getcwd(), 'app', 'plugproxy')
            os.makedirs(plugin_file, exist_ok=True)

            with open(os.path.join(plugin_file, 'manifest.json'), 'w') as archivo:
                archivo.write(manifest_json)

            with open(os.path.join(plugin_file, 'background.js'), 'w') as archivo:
                archivo.write(background_js % (ip, port, user, pwd))

            # Finally, loading plugin...
            chrome_options.add_argument(f"--load-extension={plugin_file}")

        driver = uc.Chrome(options=chrome_options)
        driver.set_page_load_timeout(self.load_timeout)

        return driver
    
    def close_browser(self, driver):
        try:
            driver.quit()
        except Exception as e:
            print(f"Error while closing browser: {e}")
        
        
