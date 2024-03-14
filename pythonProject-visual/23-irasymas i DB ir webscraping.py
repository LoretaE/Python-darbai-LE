from bs4 import BeautifulSoup
import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database(database_name, user, password):
    connection = psycopg2.connect(
        dbname='postgres',
        user=user,
        password=password,
        host='localhost'
    )
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database_name)))
    cursor.close()
    connection.close()


def create_table(database_name, user, password):
    connection = psycopg2.connect(
        dbname=database_name,
        user=user,
        password=password,
        host='localhost'
    )
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS houses(
        id SERIAL PRIMARY KEY,
        title VARCHAR,
        price DECIMAL(10, 2))  
    ''')
    connection.commit()
    print('Lentele sukurta sekmingai')
    cursor.close()
    connection.close()


def insert_data(database_name, data, user, password):
    connection = psycopg2.connect(
        dbname=database_name,
        user=user,
        password=password
    )
    cursor = connection.cursor()
    for house in data:
        cursor.execute('INSERT INTO houses (title, price) VALUES (%s, %s)', (house['title'], house['price']))
    connection.commit()
    print('Duomenys sekmingai irasyti.')
    cursor.close()
    connection.close()


def get_data(url, headers):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    data = []
    objektai = soup.find_all('article', class_="property_block clearfix")
    for objektas in objektai:
        title = objektas.find('div', class_='title_1').text.strip()
        price = objektas.find('span', class_='price').text.strip().replace(',', '')
        price = int(price)
        data.append({
            'title': title,
            'price': price
             })
    return data


def main():
    url = 'https://www.spainhouses.net/en/rent-flats-malaga.html'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/58.0.3029.110 Safari/537.3'}
    database_name = 'spain_houses'
    user = 'postgres'
    password = '3128'
    data = get_data(url, headers)
    create_database(database_name, user, password)
    create_table(database_name, user, password)
    insert_data(database_name, data, user, password)


if __name__ == '__main__':
    main()
