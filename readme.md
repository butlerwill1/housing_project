# UK Land Registry Data Analysis with Socio-Economic Merge

## Project Overview

This project leverages UK Land Registry data, enriching it with socio-economic information by postcode district to generate location based insights. It involves several key components, including Apache Spark Big Data processing, geospatial merging, and visualization through a Streamlit dashboard, aiming to provide a comprehensive understanding of property transaction dynamics in relation to socio-economic factors.

## Features

- **Data Processing**: Functions are implemented to clean and prepare the UK Land Registry data for analysis, ensuring quality and consistency.
- **Geospatial Merging**: Utilizes geopandas for merging land registry data with socio-economic data on a postcode district level, enabling spatial analysis of socio-economic impacts on property transactions.
- **PySpark Analytics**: Employs PySpark for efficient processing of large datasets, facilitating complex analytics tasks such as grouping and aggregation by postcode district.
- **Visualization Dashboard**: A Streamlit-based dashboard presents interactive maps and charts, offering users the ability to explore data through various lenses such as price changes, property types, and socio-economic indicators.

## Data Sources

- **Price Paid HM Land Registry**: Sales prices of properties in England and Wales from 1995. The file is around 5Gb and can be downloaded [here](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) Read more in the LandRegistryDataDoc.md file.
- **Postcode District Polygons**: Polygons in shapely [files](https://datashare.ed.ac.uk/handle/10283/2597) defining Postcode Areas, Districts and Sectors. From Edinburgh DataShare.
- **English Incides of Deprivation - Socio-economic Data** [Statistics](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019) on relative deprivation in small areas in England. Gives the statistics in a shapely file. Read more in the SocioEconomicDataDoc.md file.

## Built With
- **AWS EMR Clusters**
- **Terraform**
- **Apache Spark**
- **Geopandas**
- **Streamlit**
- **Folium**

## Each Code File Explanation
- **functions.py**: Provides foundational utility functions for data cleaning, aggregation, and preprocessing. It's used across various scripts for consistent data manipulation tasks.
- **pyspark_functions.py**: Defines PySpark functions for processing large datasets, including splitting postcodes, calculating statistical measures, and evaluating sample quality. It supports complex data transformations and analyses, such as calculating price changes and rolling averages.
- **preprocessing_qa.py**: Focused on quality assurance for land registry and socio-economic datasets, this script identifies and addresses missing or inaccurate data. It validates postcodes, prices, and dates in the land registry data and checks for invalid geometries in geospatial datasets. The script ensures data integrity before further processing and analysis.
- **transaction_groupby.py**: Utilizes PySpark to aggregate land registry data by geographical levels, applying quality metrics to ensure data reliability. It calculates average prices and percentage price changes, filtering the data based on quality criteria. The script exports processed datasets for further analysis or visualization, serving as a foundational step in the data workflow.
- **qa_groupby_data.py**: Performs quality assurance on the grouped transaction data and prepares it for analysis or visualization. It filters the transaction data by property type and postcode districts that have enough transactions for a significant sample size. The script exports cleaned and processed data for visualization, especially in the Streamlit dashboard.
- **geospatial_merge.py**: This script merges postcode district polygons with socio-economic indicators, preparing geospatial data for analysis. It adjusts geographic coordinate systems, renames columns for clarity, and performs spatial joins to combine datasets. The result is a GeoDataFrame that enriches postcode districts with socio-economic data, exported for further use in visualizations or analysis.
- **streamlit_dash.py**: Creates an interactive dashboard using Streamlit, integrating geospatial and transaction data for visualization. It features map-based visualizations and statistical charts to explore property prices and transactions. This script makes the processed data accessible and interpretable to end-users, highlighting market trends and socio-economic insights.

