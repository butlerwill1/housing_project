# UK Land Registry Data Analysis with Socio-Economic Merge

## Project Overview

This project leverages UK Land Registry data, enriching it with socio-economic information by postcode district to uncover insightful correlations and patterns. It involves several key components, including data processing, geospatial merging, analytics with PySpark, and visualization through a Streamlit dashboard, aiming to provide a comprehensive understanding of property transaction dynamics in relation to socio-economic factors.

## Features

- **Data Processing**: Functions are implemented to clean and prepare the UK Land Registry data for analysis, ensuring quality and consistency.
- **Geospatial Merging**: Utilizes geopandas for merging land registry data with socio-economic data on a postcode district level, enabling spatial analysis of socio-economic impacts on property transactions.
- **PySpark Analytics**: Employs PySpark for efficient processing of large datasets, facilitating complex analytics tasks such as grouping and aggregation by postcode district.
- **Visualization Dashboard**: A Streamlit-based dashboard presents interactive maps and charts, offering users the ability to explore data through various lenses such as price changes, property types, and socio-economic indicators.

## Data Sources

- **Price Paid HM Land Registry**: Sales prices of properties in England and Wales from 1995. The file is around 5Gb and can be downloaded [here] (https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)
- **Postcode District Polygons**: Polygons in shapely [files](https://datashare.ed.ac.uk/handle/10283/2597) defining Postcode Areas, Districts and Sectors. From Edinburgh DataShare.
- **English Incides of Deprivation - Socio-economic Data** [Statistics] (https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019) on relative deprivation in small areas in England. Gives the statistics in a shapely file.
