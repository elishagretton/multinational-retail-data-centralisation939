# Multinational Retail Data Centralisation

## Table of Contents

- [Project Brief](#project-brief)
  - [Data](#data)
  - [Key Tools](#key-tools)
  - [Key Objectives](#key-objectives)
- [Installation instructions](#installation-instructions)
  - [Set up environment](#set-up-environment)
  - [Connect to local database in pgAdmin4](#connect)
- [Usage instructions](#usage-instructions)
  - [File Structure]
  - [Extract, clean, and upload dataframes](#)
  -
- [File structure](#file-structure)
- [License Information](#license-information)

## Project Brief

A multinational company sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable.

In this project, sales data is collected and extracted from multiple sources. The data is then cleaned and uploaded to a SQL database, where the data can be managed effectively to provide data-driven insights on the company.

### Data

There are five different data sources that we connect and extract data from to gather insights on the retail data. They cover data on the `products`, `orders`, `dates` (of orders), `store_details`, and `card_details`.

The data is extracted from:

- two tables of an SQL database hosted on AWS RDS
- one table stored as a .pdf file hosted on AWS S3
- one table stored as a .csv file hosted on AWS S3
- one table stored as a .json file hosted on AWD S3
- a series of JSON objects available via an API.

### Key Tools

#### pandas

[pandas](https://pandas.pydata.org/) is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool, built on top of the Python programming language. A pandas dataframe is a two-dimensional data structure, similar to a table. The library has many in-built methods making it easy to manipulate and visualise large sets of data.

```python
# It is common for pandas to be abbreviated to pd
import pandas as pd
```

This project utilises many of pandas built in methods. A few common examples are listed below:

```python
# Read SQL table from database connection established using SQLAlchemy
dataframe = pd.read_sql_table(table_name, engine)
# Read contents of csv into DataFrame
dataframe = pd.read_csv('file.csv')
# Read contents of json into DataFrame
dataframe = pd.read_json('file.json')
# Drop rows containing null values
dataframe.dropna(inplace=True)
# Convert a DataFrame column to datetime type
pd.to_datetime(dataframe['column_name'])
# Converts a DataFrame column to string type
dataframe['column_name'].astype('string')
# Replace a character with a new character in a DataFrame column
dataframe['column_name'].str.replace('char', 'new_char')
# Set values to nan if value in DataFrame column does not follow pattern
dataframe.loc[~dataframe['column_name'].str.match(pattern), 'column_name'] = np.nan
```

#### numPy

[numPy](https://numpy.org/) serves as the essential package for scientific computing in Python, providing speed and versatility through its array computing standards. It includes a wide range of comprehensive mathematical functions, random number generators, linear algebra routines, Fourier transforms, and more.

```python
# It is common for numpy to be abbreviated to np
import numpy as np
```

In this project, we have numPy to set nan values.

```python
# An example from src/data_cleaning.py
# Set values to nan if the country_code length is larger than 2.
df.loc[df.country_code.str.len() > 2, 'country_code'] = np.nan
```

#### SQLAlchemy

#### PyYAML

#### boto3

#### requests

#### tabula-py

## Key Objectives

In this project, we learn:

1. How to connect and extract data from multiple sources e.g. a csv file, a PDF, an S3 Bucket, an API, and a json file.
2. How to clean data effectively by considering NULL values, duplicates, valid data types, incorrectly typed values, wrong information, and formatting.
3. How to organise code effectively into suitable classes to connect, extract, and clean data.
4. How to query the data using pgAdmin 4 to cast data types, manipulate the tables, and set primary and foreign keys.
5. How to query the data using pgAdmin 4 to generate insights such as:

- Which locations have the most stores?
- Which month produced the largest number of sales?
- How many sales are coming from online?
- What percentage of sales come through each type of store?

## Installation instructions

To get started with this project, follow these steps to set up your environment:

### 1. Clone the repository

```bash
git clone https://github.com/elishagretton/multinational-retail-data-centralisation939.git
cd multinational-retail-data-centralisation939
```

### 2. Set up a virtual environment

```bash
# On Unix or MacOS
python3 -m venv venv

# On Windows
python -m venv venv
```

### 3. Activate the virtual environment

```bash
# On Unix or MacOS
source venv/bin/activate

# On Windows
.\venv\Scripts\activate
```

### 4. Install the dependencies.

```bash
pip install -r requirements.txt
```

### 5. Run the project.

To run each file, use

```bash
cd src
python database_utils.py
python data_cleaning.py
python data_extraction.py
```

To view the tasks from the Software Engineering bootcamp, the tasks are split into preprocessing and queries sections.

- The `preprocessing` folder contains a .ipynb files that connect, extract, clean each data source (card_details, dates, orders, products, store_details, and users), and upload to a table in pgAdmin 4.
- The `queries` folders contains a `database_schema` folder that contains individual .sql files to create the schema. There is also a `data_insights.sql` file which interrogates the data and generates insights.

## Usage instructions

Please follow above instruction on how to install the project.

In order to use the project, please refer to the `tasks` directory. This contains a `preprocessing` and `queries` section that cover specific tasks related to this project.

There are 5 data sources: `card_details`, `dates`, `orders`, `products`, `store_details`, and `users`.
Please run the corresponding .ipynb files found in `tasks/preprocessing` folder to extract, clean, and upload the data to SQL.
Following this, please head to `tasks/queries/database_schema` to query and manipulate the data.
To generate insights from the tables, go to `tasks/queries/data_insights.sql`.

## File structure

The project is structured as follows:

- **/src**: Contains source code files.

  - `database_utils.py`: Utility functions for database operations such as connecting to data sources.
  - `data_extractor.py`: Handles data extraction.
  - `data_cleaning.py`: Manages data cleaning once data has been extracted from source.

- **/tasks**: Includes task files.

  - **/preprocessing**: Includes .ipynb preprocessing files that extract, clean, and upload tables to SQL.
    - `card_details.ipynb`
    - `dates.ipynb`
    - `orders.ipynb`
    - `products.ipynb`
    - `store_details.ipynb`
    - `orders.ipynb`
  - **/queries**: Includes creation of database schema and data insights.
    - **/database_schema**: Individual .sql files to query the data (cast new types, manipulate tables, and set primary and foreign keys.)
      - `card_details.sql`
      - `dates.sql`
      - `orders.sql`
      - `products.sql`
      - `store_details.sql`
    - `data_insights.sql`: SQL file that generates insights from the data tables.

- **.gitignore**: Specifies files and directories to ignore in version control.

- **README.md**: Documentation file with essential information.

- **LICENSE.txt**: File containing information on MIT License used in this project.
- **requirements.txt**: File containing modules to install.

## License Information

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.
