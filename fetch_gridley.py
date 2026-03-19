# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime


def get_today_grid():

    today = datetime.date.today().strftime("%Y-%m-%d")
    url = "https://gridleygame.com/" + today

    print("Opening:", url)

    options = Options()
    options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"
    options.add_argument("-headless")

    driver = webdriver.Firefox(
        executable_path=r"E:\afl-assistant\geckodriver.exe",
        options=options
    )

    driver.get(url)

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

    # IMPORTANT:
    # Template expects first value across top, second down left
    return cols, rows


if __name__ == "__main__":

    grid_rows, grid_cols = get_today_grid()

    print("\nTOP CLUES:")
    for r in grid_rows:
        print("-", r)

    print("\nLEFT CLUES:")
    for c in grid_cols:
        print("-", c)