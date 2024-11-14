import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# from telnetlib import EC

import pandas as pd
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from datetime import datetime
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')


def filter1():
    browser = webdriver.Chrome(options=options)
    browser.get('https://www.mse.mk/mk/stats/symbolhistory/kmb')
    dropdown_list = browser.find_element(By.CSS_SELECTOR, '#Code.form-control')
    # print(dropdown_list)
    select = Select(dropdown_list)
    issuers1 = select.options
    issuers = []
    for issuer in issuers1:
        flag = 0
        for ch in issuer.text:
            if ch.isdigit():
                flag = 1
        if flag == 0:
            issuers.append(issuer)
    return issuers


def filter2(issuer):
    browser = webdriver.Chrome(options=options)
    file_path = 'C:\\Users\\rulev\\PycharmProjects\\dians_prva_domasna\\' + issuer.text + '.csv'
    if os.path.exists(file_path):
        data_loaded = pd.read_csv(file_path)
        date = data_loaded.iloc[0, 0]
        return date
    else:
        url = 'https://www.mse.mk/mk/stats/symbolhistory/' + issuer.text.lower()
        todays_date = datetime.today()
        day = todays_date.day + 1
        month = todays_date.month
        browser.get(url)
        data = []
        for i in range(11):
            year = todays_date.year - i
            datef = str(day) + '.' + str(month) + '.' + str(year)
            datet = str(day - 1) + '.' + str(month) + '.' + str(year + 1)
            data_row = collect_data(browser, datef, datet)
            if data_row is not None:
                data.extend(data_row)

        df = pd.DataFrame(data)
        # print(df)
        df.to_csv('C:\\Users\\rulev\\PycharmProjects\\dians_prva_domasna\\' + issuer.text + '.csv', index=False)
        if not df.empty:
            return df.iloc[0, 0]
        else:
            return None


def filter3(issuer, date):
    browser = webdriver.Chrome(options=options)
    url = 'https://www.mse.mk/mk/stats/symbolhistory/' + issuer.text.lower()
    browser.get(url)
    today = datetime.today().strftime('%d.%m.%Y')
    if date != today and date is not None:
        file_path = 'C:\\Users\\rulev\\PycharmProjects\\dians_prva_domasna\\' + issuer.text + '.csv'
        data_loaded = pd.read_csv(file_path)
        new_data = collect_data(browser, date, today)
        if new_data is not None:
            new_data_df = pd.DataFrame(new_data)
            data = pd.concat((data_loaded, new_data_df), ignore_index=True)
        data.to_csv(file_path, index=False)


def collect_data(browser, datef, datet):
    try:
        WebDriverWait(browser, 4).until(
            EC.presence_of_element_located((By.ID, 'FromDate'))
        )
    except TimeoutException:
        return
    from_date = browser.find_element(By.ID, 'FromDate')
    from_date.clear()
    from_date.send_keys(datef)
    to_date = browser.find_element(By.ID, 'ToDate')
    to_date.clear()
    to_date.send_keys(datet)
    show = browser.find_element(By.CSS_SELECTOR, '.container-end >input')
    browser.execute_script('arguments[0].scrollIntoView();', show)
    show.click()
    # time.sleep(12)
    try:
        WebDriverWait(browser, 4).until(
            EC.presence_of_element_located((By.ID, 'resultsTable'))
        )
    except TimeoutException:
        return

    soup = BeautifulSoup(browser.page_source, 'html.parser')
    table = soup.select_one('#resultsTable')

    rows = table.select('tr')
    year_rows = []
    for row in rows:
        cells = row.select('td')
        if cells and cells[7].text != '0':
            data_row = {
                'Датум': cells[0].text,
                'Цена на последна трансакција': cells[1].text,
                'Макс.': cells[2].text,
                'Мин.': cells[3].text,
                '%пром.': cells[4].text,
                'Количина': cells[5].text,
                'Промет во БЕСТ во денари': cells[6].text,
                'Вкупен промет во денари': cells[7].text
            }
            year_rows.append(data_row)
    return year_rows


def process_issuer(issuer):
    date = filter2(issuer)
    if date:
        filter3(issuer, date)
    return issuer.text


def pipe():
    start_time = time.time()

    issuers = filter1()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_issuer, issuer) for issuer in issuers]

        for future in as_completed(futures):
            try:
                issuer_text = future.result()  # Returns issuer name
                print(f"Completed processing for issuer: {issuer_text}")
            except Exception as e:
                print(f"An error occurred: {e}")

    # for issuer in issuers:
    #    date = filter2(issuer)
    #    filter3(issuer, date)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time taken to fill the database: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    pipe()
