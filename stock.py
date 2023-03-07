import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

# Database credentials
db_user_name = 'postgres'
db_password = 'Smiley12'
host = 'localhost'
port = 5432
db_name = 'companies_stock_db'

header = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

# Page Extraction layer
def get_page(page_number):
    base_url = 'https://afx.kwayisi.org/ngx/'
    url = base_url + '?page=' + str(page_number)
    response = requests.get(url, headers=header)
    if response.status_code != 200:
        print(f'{url} returned an error {response.status_code}')
    else:
        page_content = response.text
        doc = bs(page_content, 'lxml')
        return doc

# Data Extraction layer
def extract_data():
    table_data = []
    for page_number in range(1, 3):
        doc = get_page(page_number)
        table_tags = doc.find_all('div', class_='t')
        for table_tag in table_tags:
            html_data = str(table_tag.find('table'))
            table_data.append(html_data)
    df = pd.concat(pd.read_html('\n'.join(table_data)))
    df.to_csv('data/companies_stock_info.csv', index=False)
    print('Data successfully written to csv file')
    
# Data load transfor layer
def transform_load_to_db():
    data = pd.read_csv('data/companies_stock_info.csv')
    engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@{host}:{port}/{db_name}')
    data.to_sql('company_stock', con=engine, if_exists='replace', index=False)
    print('Data successfully written to postgreSQL database')

def main():
    get_page('page_number')
    extract_data()
    transform_load_to_db()

main()
