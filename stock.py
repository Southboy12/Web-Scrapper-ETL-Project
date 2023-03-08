import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
from datetime import datetime
import dotenv, os
from dotenv import dotenv_values

def get_database_conn():
    dotenv.load_dotenv('C:/Users/MENKA/Downloads/My-project/Environment files/.env')
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    return create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')


header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

# Page Extraction layer
def extract_data(page_number):
    table_data = []
    for page_number in range(1, 3):
        base_url = 'https://afx.kwayisi.org/ngx/'
        url = base_url + '?page=' + str(page_number)
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            print(f'{url} returned an error {response.status_code}')
        else:
            page_content = response.text
            doc = bs(page_content, 'lxml')
            table_tags = doc.find_all('div', class_='t')
            for table_tag in table_tags:
                html_data = str(table_tag.find('table'))
                table_data.append(html_data)
    df = pd.concat(pd.read_html('\n'.join(table_data)))
    df.to_csv('extract_data/extracted_stock_info.csv', index=False)
    print('Data successfully extracted to csv file')

# data Transform layer
def transform_data():
    # Read the extracted csv into a DataFrame
    df = pd.read_csv('extract_data/extracted_stock_info.csv')

    # Replace null values in volume colume with mean volume of all companies for the day
    volume_mean = df.Volume.mean().round(0)
    df.Volume.fillna(value=volume_mean, inplace=True)

    # Add a new "Date" column with the current date
    date_today = datetime.now().strftime('%Y-%m-%d')
    df['Date'] = date_today

    # Write the transformed data to csv file
    df.to_csv('transformed_data/transformed_stock_info.csv', index=False)
    print('Data successfully trandformed and written to csv file')
    
# Data load transfor layer
def load_data():
    # Read the transformed csv into a DataFrame
    df = pd.read_csv('transformed_data/transformed_stock_info.csv')
    engine = get_database_conn()
    df.to_sql('company_stock', con=engine, if_exists='append', index=False)
    print('Data successfully written to postgreSQL database')

def main():
    extract_data('page_number')
    transform_data()
    load_data()

main()
