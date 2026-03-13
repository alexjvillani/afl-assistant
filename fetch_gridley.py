# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import datetime
import time


def get_today_grid():

    # ---------------------------------
    # Build today's Gridley URL
    # ---------------------------------

    today = datetime.date.today().strftime("%Y-%m-%d")
    url = "https://gridleygame.com/" + today

    print "Opening:", url

    # ---------------------------------
    # Firefox options
    # ---------------------------------

    options = Options()

    # location of Firefox
    options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"

    # run browser silently
    options.add_argument("-headless")

    # ---------------------------------
    # Start browser
    # ---------------------------------

    driver = webdriver.Firefox(
        executable_path="geckodriver.exe",
        firefox_options=options
    )

    driver.get(url)

    # allow React page to render
    time.sleep(4)

    # ---------------------------------
    # Find grid buttons
    # ---------------------------------

    buttons = driver.find_elements_by_css_selector("button[aria-label]")

    labels = []

    for b in buttons:

        label = b.get_attribute("aria-label")

        # grid clues are long sentences
        if label and len(label) > 40:
            labels.append(label)

    driver.quit()

    # ---------------------------------
    # Gridley is always 3x3
    # ---------------------------------

    rows = labels[:3]
    cols = labels[3:6]

    return rows, cols


# ---------------------------------
# Allow running script directly
# ---------------------------------

if __name__ == "__main__":

    rows, cols = get_today_grid()

    print "\nROWS:"
    for r in rows:
        print "-", r

    print "\nCOLS:"
    for c in cols:
        print "-", c