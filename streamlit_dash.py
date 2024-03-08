#%%
import streamlit as st
# import pydeck as pdk
import folium
from folium.features import GeoJson, GeoJsonTooltip
import geopandas as gpd
from streamlit_folium import st_folium
import altair as alt
import pandas as pd
st.set_page_config(layout="wide")
#%%-----------------------------------------------------------------------------------------------------
#                           Streamlit Dashboard with Map and Price Change Graph
#-------------------------------------------------------------------------------------------------------

# Decorate the function with st.cache to only run it once and cache the result
@st.cache_data()
def load_district_groupby_socio_economic():
    """Load the data source which contains the transaction data results by postcode district
    for 2023 with the 2019 socio economic data aggregated up to the district level joined on."""
    gdf = gpd.read_file('district_groupby_socio_economic.gpkg', layer='socio')

    return gdf

@st.cache_data()
def load_socio_economic():
    """Load the 2019 socio-economic dataset with polygons the original smaller area level"""
    socio_economic = gpd.read_file("socio_economic_postcode.gpkg")

    return socio_economic

@st.cache_data()
def load_price_graph():
    """Load the dataset of the average price of property for every year for each postcode district"""
    price_graph = pd.read_csv("district_groupby_price_graph.csv")

    return price_graph

district_groupby_socio_economic = load_district_groupby_socio_economic()
socio_economic = load_socio_economic()
price_graph = load_price_graph()

print("Datasets read in")
#%%
st.title('UK Property Transaction Dataset With Socio Economic Data')

col1, col2 = st.columns(2)

with col1:
    london_or_not = st.multiselect("London Or Outside London?", 
                                   sorted(district_groupby_socio_economic['is_london?'].unique()),
                                   default='Greater London')

    district_choices = st.multiselect("Select Postcode District", 
                                  sorted(district_groupby_socio_economic[district_groupby_socio_economic['is_london?'].isin(london_or_not)]['PostDist'].unique()),
                                  default='E3')
    
    socio_tooltip_choices = st.multiselect("Select 2019 Socio-economic tooltip options for smaller polygons", 
                                    sorted(socio_economic.columns),
                                  default=['AreaName', 'CrimeScore', 'EnvironmentScore', 'OutdoorLivingScore'])
# %%
district_groupby_socio_economic = district_groupby_socio_economic[ \
                        district_groupby_socio_economic['PostDist'].isin(district_choices)]

socio_economic = socio_economic[socio_economic['PostDist'].isin(district_choices)]
#%%

# Filter the GeoDataFrame based on the selected districts
selected_districts = district_groupby_socio_economic[
    district_groupby_socio_economic['PostDist'].isin(district_choices)
]

# Calculate the bounds of the selected polygons
bounds = selected_districts.geometry.total_bounds
minx, miny, maxx, maxy = bounds

# Calculate the center of the bounds
center = [(miny + maxy) / 2, (minx + maxx) / 2]

# Initialize the map at the center of the bounds
m = folium.Map(location=center)

# Fit the map to the bounds
m.fit_bounds([[miny, minx], [maxy, maxx]])

#%%

# Define the tooltip for the postcode district level polygons
postcode_tooltip = GeoJsonTooltip(
    fields=['PostDist', '2023_rolling_5_average', 'CrimeAvg'],
    aliases=['Postcode District:', '2023_rolling_5_average', 'CrimeAvg'],  # this is the label that will be shown in the tooltip
    localize=True,
    sticky=False,  # Set to True for the tooltip to follow the mouse
    labels=True,
    style="""
        background-color: #F0EFEF;
        border: 2px solid black;
        border-radius: 3px;
        box-shadow: 3px;
    """,
    max_width=800,
)

# Define the tooltip for the lower level socio economic polygons
socio_tooltip = GeoJsonTooltip(
    fields=socio_tooltip_choices,
    aliases=socio_tooltip_choices,  # this is the label that will be shown in the tooltip
    localize=True,
    sticky=False,  # Set to True for the tooltip to follow the mouse
    labels=True,
    style="""
        background-color: #F0EFEF;
        border: 2px solid black;
        border-radius: 3px;
        box-shadow: 3px;
    """,
    max_width=800,
)

# Add the postcode district level polygons
GeoJson(
    district_groupby_socio_economic,
    style_function=lambda x: {'fillColor': '#ffffff00'},  # Transparent polygons
    tooltip=postcode_tooltip
).add_to(m)

# Add the socio_economic dataset smaller polygons (the ones that fit within the postcode district)
GeoJson(
    socio_economic,
    style_function=lambda x: {
        'fillColor': 'rgba(255, 0, 0, 0.5)',  # Semi-transparent red
        'color': 'rgba(255, 0, 0, 0.5)',       # Outline color, you can adjust as needed
        'weight': 1,                            # Outline weight, you can adjust as needed
        'fillOpacity': 0.5                      # Adjust fill opacity here as well
    },
    tooltip=socio_tooltip
).add_to(m)

# Display the map in Streamlit
with col1:
    st_folium(m, width=700, height=500)


default_display_cols = ['postcode_district', 'avg_price', '2023_rolling_5_average', 'CrimeAvg']

with col2:

    property_type = st.multiselect("Select Property Type", 
                   options=sorted(district_groupby_socio_economic.property_type), 
                   default='F')
    
    display_cols = st.multiselect("Select Info Columns For Socio Economic Data Aggregated to Postcode District Level", 
                   options=sorted(district_groupby_socio_economic.columns), 
                   default=default_display_cols)
    
    # Display a dataframe of the selected metrics for comaprison between districts
    st.dataframe(district_groupby_socio_economic[district_groupby_socio_economic['year']==2023][display_cols],
                 use_container_width=True,
                 hide_index=True)
    
    # Display a graph of how the average price has changed over the years
    price_graph = price_graph[price_graph['postcode_district'].isin(district_choices)]
    price_graph['year'] = pd.to_datetime(price_graph['year'], format='%Y')

    # Create an interactive line chart
    chart = alt.Chart(price_graph).mark_line().encode(
        x='year:T',  # The ':T' tells Altair that the data is temporal
        y=alt.Y('avg_price:Q', title='Average Price'),  # The ':Q' tells Altair that the data is quantitative
        color=alt.Color('postcode_district:N', legend=alt.Legend(title="Postcode District")),  # Different line for each postcode_district
        tooltip=['postcode_district:N', 'year:T', 'avg_price:Q', 'num_transactions:Q']  # Tooltips for interactivity
    ).interactive()

    # Display the chart in the Streamlit app
    st.altair_chart(chart, use_container_width=True)
# %%