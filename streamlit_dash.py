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
# pdk.settings.mapbox_key = "pk.eyJ1IjoiYnV0bGVyd2lsbDEiLCJhIjoiY2xzeWtkZTRkMGF2NzJsbzJuZnd1NzBybCJ9.tOLsN0JQNHMczznmiPrAuQ"
#%%-----------------------------------------------------------------------------------------------------
#                           Streamlit Dashboard with Map and Price Change Graph
#-------------------------------------------------------------------------------------------------------

# district_groupby_socio_economic = gpd.read_file('district_groupby_socio_economic.gpkg', layer='socio')
# Decorate the function with st.cache to only run it once and cache the result
@st.cache_data()
def load_district_groupby_socio_economic():
    # gdf = gpd.read_file("GB_Postcodes/PostalDistrict.shp")
    gdf = gpd.read_file('district_groupby_socio_economic.gpkg', layer='socio')
    # gdf = gdf.to_crs("EPSG:4326")
    return gdf

@st.cache_data()
def load_socio_economic():
    socio_economic = gpd.read_file("socio_economic_postcode.gpkg")

    return socio_economic

@st.cache_data()
def load_price_graph():
    price_graph = pd.read_csv("district_groupby_price_graph.csv")

    return price_graph

# Call the function to get the GeoDataFrame
district_groupby_socio_economic = load_district_groupby_socio_economic()
socio_economic = load_socio_economic()
price_graph = load_price_graph()

print("Datasets read in")
#%%
st.title('Postcode Lottery')
col1, col2 = st.columns(2)
with col1:
    london_or_not = st.multiselect("London Or Outside London?", 
                                   sorted(district_groupby_socio_economic['is_london?'].unique()),
                                   default='Greater London')

    district_choices = st.multiselect("Select Postcode District", 
                sorted(district_groupby_socio_economic[district_groupby_socio_economic['is_london?'].isin(london_or_not)]['PostDist'].unique()),
                                  default='E3')
    
    socio_tooltip_choices = st.multiselect("Select Socio-economic tooltip options for smaller polygons", 
                sorted(socio_economic.columns),
                                  default=['AreaName', 'CrimeScore', 'EnvironmentScore', 'OutdoorLivingScore'])
# %%
district_groupby_socio_economic = district_groupby_socio_economic[ \
                        district_groupby_socio_economic['PostDist'].isin(district_choices)]

socio_economic = socio_economic[socio_economic['PostDist'].isin(district_choices)]
#%%
# district_groupby_socio_economic_projected = district_groupby_socio_economic.to_crs('EPSG:3857')
# Now you can calculate the centroid without the warning
longitude = district_groupby_socio_economic.geometry.centroid.x.mean()
latitude = district_groupby_socio_economic.geometry.centroid.y.mean()

# geojson = district_groupby_socio_economic.__geo_interface__

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
    
    display_cols = st.multiselect("Select Info Columns", 
                   options=sorted(district_groupby_socio_economic.columns), 
                   default=default_display_cols)
    
    st.dataframe(district_groupby_socio_economic[district_groupby_socio_economic['year']==2023][display_cols],
                 use_container_width=True,
                 hide_index=True)
    
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
# Set the viewport for the map
# viewport = pdk.ViewState(latitude=latitude, 
#                          longitude=longitude, 
#                          zoom=10)

# # Create a layer for the polygons
# layer = pdk.Layer('GeoJsonLayer', 
#                   data=geojson,
#                   get_fill_color=[255, 170, 0, 200],  # RGBA color format
#                   pickable=True)

# # Render the map
# st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=viewport))
# %%
