import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from datetime import datetime
import os

BASE_URL = 'https://www.mse.mk/mk/stats/symbolhistory/'



def filter1():
    url = 'https://www.mse.mk/mk/stats/symbolhistory/kmb'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    dropdown_list = soup.select('#Code.form-control option')
    issuers = [option for option in dropdown_list if not any(char.isdigit() for char in option.text)]
    return issuers


def filter2(issuer):
    file_path = f'C:\\Users\\rulev\\PycharmProjects\\dians_prva_domasna\\issuers\\{issuer.text}.csv'

    if os.path.exists(file_path):
        data_loaded = pd.read_csv(file_path)
        date = data_loaded.iloc[0, 0]
        return date
    else:
        url = BASE_URL + issuer.text.lower()
        todays_date = datetime.today()
        day = todays_date.day + 1
        month = todays_date.month

        data = []
        for i in range(11):
            year = todays_date.year - i
            datef = f"{day}.{month}.{year}"
            datet = f"{day - 1}.{month}.{year + 1}"
            data_row = collect_data(url, datef, datet)
            if data_row:
                data.extend(data_row)

        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        return df.iloc[0, 0] if data else None


def collect_data(url, datef, datet):
    payload = {
        'FromDate': datef,
        'ToDate': datet,
    }
    response = requests.post(url, data=payload)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.select_one('#resultsTable')
    rows = table.select('tr') if table else []
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


def filter3(issuer, date):
    url = BASE_URL + issuer.text.lower()
    today = datetime.today().strftime('%d.%m.%Y')
    if date != today and date is not None:
        file_path = f'C:\\Users\\rulev\\PycharmProjects\\dians_prva_domasna\\issuers\\{issuer.text}.csv'
        data_loaded = pd.read_csv(file_path)
        new_data = collect_data(url, date, today)
        if new_data:
            new_data_df = pd.DataFrame(new_data)
            data = pd.concat((data_loaded, new_data_df), ignore_index=True)
            data.to_csv(file_path, index=False)


def process_issuer(issuer):
    date = filter2(issuer)
    if date:
        filter3(issuer, date)
    return issuer.text


def pipe(progress_callback=None):
    start_time = time.time()
    issuers = filter1()

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(process_issuer, issuer) for issuer in issuers]

        for idx, future in enumerate(as_completed(futures)):
            try:
                issuer_text = future.result()
                if progress_callback:
                    progress_callback(
                        progress=int(((idx + 1) / len(issuers)) * 100),
                        message=f"Completed processing for issuer: {issuer_text}"
                    )
            except Exception as e:
                if progress_callback:
                    progress_callback(message=f"Error processing issuer: {str(e)}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    if progress_callback:
        progress_callback(message=f"Scraping completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    pipe()
