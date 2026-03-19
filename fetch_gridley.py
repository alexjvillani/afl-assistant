# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import datetime
import os


def get_today_grid():

    today = datetime.date.today().strftime("%Y-%m-%d")
    url = "https://gridleygame.com/" + today

    print("Opening:", url)

    options = Options()
    options.add_argument("-headless")

    # 👇 looks for geckodriver.exe in same folder as this script
    driver_path = os.path.join(os.path.dirname(__file__), "geckodriver.exe")

    # 👇 Selenium 4 first, fallback to Selenium 3
    try:
        from selenium.webdriver.firefox.service import Service
        service = Service(driver_path)
        driver = webdriver.Firefox(service=service, options=options)
    except TypeError:
        # Selenium 3 fallback
        driver = webdriver.Firefox(executable_path=driver_path, options=options)

    driver.get(url)

    # Import here so both Selenium versions behave
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "[aria-label]"))
    )

    elements = driver.find_elements(By.CSS_SELECTOR, "[aria-label]")

    rows = []
    cols = []

    for e in elements:

        label = e.get_attribute("aria-label")

        if not label:
            continue

        if len(label) < 25:
            continue

        loc = e.location
        x = loc["x"]
        y = loc["y"]

        # Top clues
        if y < 300:
            cols.append((x, label))

        # Left clues
        elif x < 300:
            rows.append((y, label))

    driver.quit()

    rows.sort()
    cols.sort()

    rows = [r[1] for r in rows][:3]
    cols = [c[1] for c in cols][:3]

    print("ROWS:", rows)
    print("COLS:", cols)

    # Template expects (top, left)
    return cols, rows


if __name__ == "__main__":

    grid_rows, grid_cols = get_today_grid()

    print("\nTOP CLUES:")
    for r in grid_rows:
        print("-", r)

    print("\nLEFT CLUES:")
    for c in grid_cols:
        print("-", c)