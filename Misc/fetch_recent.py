# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import datetime
import time


def scrape_gridley(date):

    url = "https://gridleygame.com/" + date

    options = Options()
    options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"
    options.add_argument("-headless")

    driver = webdriver.Firefox(
        executable_path="geckodriver.exe",
        firefox_options=options
    )

    driver.get(url)

    time.sleep(4)

    buttons = driver.find_elements_by_css_selector("button[aria-label]")

    labels = []

    for b in buttons:

        label = b.get_attribute("aria-label")

        if label and len(label) > 40:
            labels.append(label)

    driver.quit()

    return labels


print "\nSCRAPING LAST 7 GRIDLEYS\n"

today = datetime.date.today()

for i in range(7):

    d = today - datetime.timedelta(days=i)
    date_str = d.strftime("%Y-%m-%d")

    print "GRIDLEY:", date_str

    labels = scrape_gridley(date_str)

    for l in labels:
        print "-", l

    print