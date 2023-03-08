# Jazzy Investment Data Pipeline

This is a Python web scraper that extracts the daily stock exchange data of listed companies/securities from `https://afx.kwayisi.org/ngx/` and loads it into a Postgresql database. The data pipeline consists of three stages:

Extract: Web scraping the data from the website
Transform: Cleaning and formatting the data into a format suitable for loading into the database
Load: Inserting the data into the Postgresql database

## Prerequisites

Python 3.x
pip package manager
Postgresql database

## Installation

Clone this repository to your local machine:
`git clone https://github.com/10Alytics/Jazzy-Investment-Data-Pipeline.git`

Install the required Python libraries using pip:
pip install -r requirements.txt

Create a Postgresql database with a table to store the stock exchange data. 
You can use the following SQL statement to create the table:

CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    open FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    close FLOAT NOT NULL,
    volume INTEGER NOT NULL,
    date DATE NOT NULL
);

Create a .env file in the root directory of the project and set the database connection details:
`DATABASE_URL=postgresql://user:password@host:port/database_name`

Replace the user, password, host, port, and database_name placeholders with your own database connection details.

## Usage
To run the data pipeline, execute the following command in the terminal:

`python main.py`

This will extract the stock exchange data from the website, transform it into a suitable format, and load it into the database.

By default, the pipeline will load data for the current day. You can specify a different date using the --date command-line argument. For example:

css
Copy code
python main.py --date 2022-03-07
This will load data for March 7th, 2022.

## Conclusion

With this data pipeline, you can easily extract, transform, and load the daily stock exchange data of listed companies/securities from https://afx.kwayisi.org/ngx/ into a Postgresql database. This data can be used for stock trading analytics by Jazzy Investment or any other stockbroking firm.

