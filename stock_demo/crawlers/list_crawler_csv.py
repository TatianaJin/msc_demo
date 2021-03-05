#! /usr/bin/env python3

from selenium import webdriver
import selenium.webdriver.support.ui as ui
import time
import csv
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options

with open('//data//opt//users//destiny//resource//Stock_List.csv', 'w', newline='') as stocklist:

    fields = ['Ticket', 'Company Name']
    writer = csv.DictWriter(stocklist, fields)
    writer.writeheader()

    # Connect chrome browser to open NYSE offical page
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    driver = webdriver.Chrome(executable_path="/data/opt/users/destiny/chromedriver", chrome_options=options)
    driver.get("https://www.nyse.com/listings_directory/stock")

    ############################################# Preparation is done above #############################################

    # Loop 669 times to crawl 669 pages
    # Loop n times to crawl n pages
    # Any better method to crawl till end page automatically?

    stock = ""
    name = ""
    i = 1
    j = 1

    while i <= 669:
        time.sleep(1)
        k = i + 1
        wait = ui.WebDriverWait(driver, 10)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        trlist = soup.find_all('tr')

        for tr in trlist:
            tdlist = tr.find_all('td')

            for td in tdlist:
                if ((stock != '') and (name != '')):
                    writer.writerow({'Ticket': stock, 'Company Name': name})
                    stock = ''
                    name = ''

                if j == 1:
                    stock = td.text
                    j = j + 1
                    continue

                if j == 2:
                    name = td.text
                    print(td.text)
                    j = j - 1
                    continue

        wait.until(lambda driver: driver.find_element_by_link_text(str(k)))
        next_page = driver.find_element_by_link_text(str(k))
        driver.execute_script("arguments[0].click();", next_page)
        i = i + 1
        time.sleep(1)

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    trlist = soup.find_all('tr')
