# Tuva Data Quality Dashboard

A Dash application for visualizing and monitoring healthcare data quality metrics based on the Tuva Data Quality 
Framework.

## Overview

The Tuva Data Quality Dashboard provides a comprehensive view of data quality for healthcare data mapped to the Tuva 
Data Model. Tries to answer the question of "have I mapped my input layer correctly?" It allows users to:

- Monitor overall data quality with a grading system (A-F)
- Track data mart usability status
- Identify and investigate data quality issues
- Generate printable data quality reports

This application consumes data quality test results and exploratory chart data from 
the [Tuva Health dbt package](https://github.com/tuva-health/tuva).

## Features

- **Data Quality Grade**: Overall assessment of data quality (A-F)
- **Data Mart Status**: Usability status for each data mart
- **Test Results**: Detailed view of passing and failing tests
- **Quality Dimensions**: Analysis by completeness, validity, consistency, etc.
- **Visualizations**: Charts showing data patterns and quality metrics
- **Report Generation**: Printable data quality report cards

## Getting Started

### Prerequisites

- Python 3.11+
- Docker (optional, for containerized deployment)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/tuva-health/tuva_dqi.git
   cd tuva_dqi/tuva_dqi
   ```
2. Create a virtual environment for this repo
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    pip install -r requirements.txt
    ```
3.	Run the application:
    ```bash
    python app.py
    ```
4. Access the dashboard at [localhost:8080](http://localhost:8080)

### Docker Deployment
To run the application in a Docker container:
```bash
docker build -t tuva_dqi .
docker run -p 8080:8080 tuva_dqi
```
## Data Sources
This dashboard consumes two main CSV files exported from the Tuva dbt package:
	
1.	**Test Results** (`data_quality__testing_summary`): Contains results of data quality tests
2.	**Chart Data** (`data_quality__exploratory_charts`): Contains data for visualizations

### Generating Input Data from Tuva dbt Package

The input CSV files are generated from the [Tuva dbt package](https://github.com/tuva-health/tuva). The dashboard is
designed to work with the `dqi-enhancements` branch which contains the necessary data quality components. 
Any release compatibility would require a version > 0.14.2.

### Setting up Tuva dbt Project

1. Create a new dbt project or use an existing one
2.	Add the Tuva package to your `packages.yml` file, specifying the `dqi-enhancements` branch:
```yml
packages:
  - git: https://github.com/tuva-health/tuva
    revision: dqi-enhancements  # Use the dqi-enhancements branch
```
Alternatively, you can use a specific commit hash:
```yml
packages:
  - git: https://github.com/tuva-health/tuva
    revision: "24da943e245b97971c90be7ae4ecc091d6272cc1"
```
3.	Install the package and run dbt building commands:
```bash
dbt clean
dbt deps
dbt seed --full-refresh
dbt run
dbt test
```

4.	Export the required tables as CSV files:
* Export `data_quality__testing_summary` as CSV with headers
* Export `data_quality__exploratory_charts` as CSV with headers
5.	Upload these CSV files to the dashboard using the "Import Test Results" feature


## Development Status
This dashboard is currently in alpha/early development and has not been officially released. It is designed to work 
with the `dqi-enhancements` branch of the Tuva dbt package, which contains the necessary data quality components.

