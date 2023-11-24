# Multinational Retail Data Centralisation

## Table of Contents

1. [Introduction](#introduction)
2. [Installation instructions](#installation-instructions)
3. [Usage instructions](#usage-instructions)
4. [File structure](#file-structure)
5. [License Information](#license-information)

## Introduction

A multinational company sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable.

In this project, sales data is collected and extracted from multiple sources. The data is then cleaned and uploaded to a SQL database, where the data can be managed effectively to provide data-driven insights on the company.

With this project, we learn:

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

## Usage instructions

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
      - `orders.sql`
    - `data_insights.sql`: SQL file that generates insights from the data tables.

- **.gitignore**: Specifies files and directories to ignore in version control.

- **README.md**: Documentation file with essential information.

- **LICENSE.txt**: File containing information on MIT License used in this project.
- **requirements.txt**: File containing modules to install.

## License Information

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.
