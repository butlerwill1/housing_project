#%%
import streamlit as st
# import pydeck as pdk
import folium
from folium.features import GeoJson, GeoJsonTooltip
import geopandas as gpd
from streamlit_folium import st_folium
st.set_page_config(layout="wide")
# pdk.settings.mapbox_key = "pk.eyJ1IjoiYnV0bGVyd2lsbDEiLCJhIjoiY2xzeWtkZTRkMGF2NzJsbzJuZnd1NzBybCJ9.tOLsN0JQNHMczznmiPrAuQ"
#%%

# district_groupby_socio_economic = gpd.read_file('district_groupby_socio_economic.gpkg', layer='socio')
# Decorate the function with st.cache to only run it once and cache the result
@st.cache_data()
#%%
def load_data():
    # gdf = gpd.read_file("GB_Postcodes/PostalDistrict.shp")
    gdf = gpd.read_file('district_groupby_socio_economic.gpkg', layer='socio')
    # gdf = gdf.to_crs("EPSG:4326")

    return gdf

#%% Call the function to get the GeoDataFrame
district_groupby_socio_economic = load_data()
print("Dataset read in")
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
# %%
district_groupby_socio_economic = district_groupby_socio_economic[ \
                        district_groupby_socio_economic['PostDist'].isin(district_choices)]
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


# m = folium.Map(location=[latitude, longitude], zoom_start=12)

# Create a Folium map object
# Define the tooltip to use the 'PostDist' column from your GeoDataFrame
tooltip = GeoJsonTooltip(
    fields=['PostDist', '2023_rolling_5_average', 'CriScore_Avg'],
    aliases=['Postcode District:', '2023_rolling_5_average', 'CriScore_Avg'],  # this is the label that will be shown in the tooltip
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

# Add the GeoDataFrame to the map with the tooltip
GeoJson(
    district_groupby_socio_economic,
    style_function=lambda x: {'fillColor': '#ffffff00'},  # Transparent polygons
    tooltip=tooltip
).add_to(m)

# Display the map in Streamlit
with col1:
    st_folium(m, width=700, height=500)


default_display_cols = ['postcode_district', 'avg_price', '2023_rolling_5_average', 'CriScore_Avg']

with col2:
    display_cols = st.multiselect("Select Info Columns", 
                   options=sorted(district_groupby_socio_economic.columns), 
                   default=default_display_cols)
    
    st.dataframe(district_groupby_socio_economic[district_groupby_socio_economic['year']==2023][display_cols],
                 use_container_width=True,
                 hide_index=True)
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
