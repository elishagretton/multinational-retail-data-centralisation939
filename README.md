# Multinational Retail Data Centralisation

# Table of Contents

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

1. How to connect and extract data from multiple sources e.g. a csv file, a PDF, an S3 Bucket, and an API.
2. How to clean data effectively by considering NULL values, duplicates, valid data types, incorrectly typed values, and wrong information.
3. How to organise code effectively into suitable classes to connect, extract, and clean data.
4. This list will continue growing as I complete the project!

## Installation instructions

Needs updating.

## Usage instructions

Needs updating.

## File structure

The project is structured as follows:

- **/src**: Contains source code files.

  - `database_utils.py`: Utility functions for database operations such as connecting to data sources.
  - `data_extractor.py`: Handles data extraction.
  - `data_cleaning.py`: Manages data cleaning once data has been extracted from source.

- **/public**: Holds public images and documents.

  - `card_details.pdf`: PDF document serving as a data source.

- **/tests**: Includes test files.

  - `test.ipynb`: Jupyter Notebook for testing and completing the tasks given in this project provided by Software Engineering Bootcamp with AiCore.

- **.gitignore**: Specifies files and directories to ignore in version control.

- **README.md**: Documentation file with essential information.

- **LICENSE.txt**: File containing information on MIT License used in this project.

## License Information

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.
