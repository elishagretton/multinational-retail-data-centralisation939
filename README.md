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

1. How to connect and extract data from multiple sources e.g. a csv file, a PDF, an S3 Bucket, and an API.
2. How to clean data effectively by considering NULL values, duplicates, valid data types, incorrectly typed values, wrong information, and formatting.
3. How to organise code effectively into suitable classes to connect, extract, and clean data.
4. This list will continue growing as I complete the project!

## Installation instructions

To get started with this project, follow these steps to set up your environment:

### 1. Clone the repository

```bash
git clone https://github.com/elishagretton/multinational-retail-data-centralisation939.git
cd multinational-retail-data-centralisation939
```

### 2. Set up a virtual environment (optional)

```bash
# On Unix or MacOS
python3 -m venv venv

# On Windows
python -m venv venv
```

### 3. Activate the virtual environment.

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

To view the tasks from the Software Engineering bootcamp, run below

```bash
cd tasks
python tasks.ipynb
```

## Usage instructions

Please follow above instruction on how to install the project.

In order to use the project, please refer to the `tasks` directory. This contains a Jupyter Notebook (`tasks.ipynb`) that covers specific tasks related to this project. You can find detailed explanations and solutions for these tasks in the notebook [here](tasks/tasks.ipynb).

## File structure

The project is structured as follows:

- **/src**: Contains source code files.

  - `database_utils.py`: Utility functions for database operations such as connecting to data sources.
  - `data_extractor.py`: Handles data extraction.
  - `data_cleaning.py`: Manages data cleaning once data has been extracted from source.

- **/tasks**: Includes task files.

  - `tasks.ipynb`: Jupyter Notebook for testing and completing the tasks given in this project provided by Software Engineering Bootcamp with AiCore.

- **.gitignore**: Specifies files and directories to ignore in version control.

- **README.md**: Documentation file with essential information.

- **LICENSE.txt**: File containing information on MIT License used in this project.
- **requirements.txt**: File containing modules to install.

## License Information

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.
