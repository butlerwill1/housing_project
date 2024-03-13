# UK Land Registry Data Analysis with Socio-Economic Data    Merge

## Project Overview

This project leverages UK Land Registry data, grouping it by postcode district (e.g. SW11, E3) and then enriching it with socio-economic data from 2019 to generate location based insights. The socioeconomic data is segmented into areas smaller than a postcode district, so the postcode district that the smaller areas reside within is found and then aggregated up to form socio-economic insights for the whole district. For example,aggregations on teh transaction dataset can tell us the average price for flats in SW11 in 2023, and the socio-economic data can tell us 

![Example Dashboard Output Of London Postcode District Comparisons](/Images/LondonDistrictsComparison.png)

## Features

- **Data Processing**: Functions are implemented to clean and prepare the UK Land Registry data for analysis, ensuring quality and consistency.
- **Geospatial Merging**: Utilizes geopandas for merging land registry data with socio-economic data on a postcode district level, enabling spatial analysis of socio-economic impacts on property transactions.
- **PySpark Analytics**: Employs PySpark for efficient processing of large datasets, facilitating tasks such as grouping and aggregation by postcode district.
- **Visualization Dashboard**: A Streamlit-based dashboard presents interactive maps and charts, offering users the ability to explore data through various lenses such as price changes, property types, and socio-economic indicators.

## Data Sources

- **Price Paid HM Land Registry**: Sales prices of properties in England and Wales from 1995. The file is around 5Gb and can be downloaded [here](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads) Read more in the LandRegistryDataDoc.md file.
- **Postcode District Polygons**: Polygons in shapely format defining Postcode Areas, Districts and Sectors can be downloaded
[here](https://datashare.ed.ac.uk/handle/10283/2597). From Edinburgh DataShare.
- **English Incides of Deprivation - Socio-economic Data** [Statistics](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019) on relative deprivation in small areas in England. Gives the statistics in a shapely file. Read more in the SocioEconomicDataDoc.md file.

## Built With
- **AWS EMR Clusters**: A Cloud Big Data platform for processing massive amounts of data which can host big data software technologies such as Apache Spark 
- **Terraform**: An Infrastructure as Code (IaC) technology used as a clear and convinient way to create a AWS EMR Cluster
- **Apache Spark**: An open source programming interface for big data tasks which manipulates a cluster of computers and distributed datasets to process a large amount of data
- **Geopandas**: A python library similar to pandas but also has "shapely" technology for manipulation of geometric objects and "PyProj" for projection and coordinate transformations
- **Streamlit**: A python dashboarding technology with interactive filters, buttons, widgets, maps, tables and more.
- **Folium**: A python library for making interactive data visualisations on maps utilising Leaflet.js

## Code File Explanations
- **functions.py**: Provides foundational utility functions for data cleaning, aggregation, and preprocessing. It's used across various scripts for consistent data manipulation tasks.
- **pyspark_functions.py**: Defines PySpark functions for processing large datasets, including splitting postcodes, calculating statistical measures of variation, and evaluating sample quality. It supports complex data transformations and analyses, such as calculating price changes and rolling averages.
- **preprocessing_qa.py**: Focused on quality assurance for land registry and socio-economic datasets, this script identifies and addresses missing or inaccurate data. It validates postcodes, prices, and dates in the land registry data and checks for invalid geometries in geospatial datasets. The script ensures data integrity before further processing and analysis.
- **transaction_groupby.py**: Utilizes PySpark to aggregate land registry data by geographical levels, applying quality metrics to ensure data reliability. It calculates average prices and percentage price changes, filtering the data based on quality criteria. The script exports processed datasets for further analysis or visualization, serving as a foundational step in the data workflow.
- **qa_groupby_data.py**: Performs quality assurance on the grouped transaction data and prepares it for analysis or visualization. It filters the transaction data by property type and postcode districts that have enough transactions for a significant sample size. The script exports cleaned and processed data for visualization, especially in the Streamlit dashboard.
- **geospatial_merge.py**: This script merges postcode district polygons with socio-economic indicators, preparing geospatial data for analysis. It adjusts geographic coordinate systems, renames columns for clarity, and performs spatial joins to combine datasets. The result is a GeoDataFrame that enriches postcode districts with socio-economic data, exported for further use in visualizations or analysis.
- **streamlit_dash.py**: Creates an interactive dashboard using Streamlit, integrating geospatial and transaction data for visualization. It features map-based visualizations and statistical charts to explore property prices and transactions. This script makes the processed data accessible and interpretable to end-users, highlighting market trends and socio-economic insights.

