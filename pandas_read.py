#%%----------------------------------------------------------------------------------------------------
#                                       Pandas test area
#%%----------------------------------------------------------------------------------------------------

import pandas as pd
import regex as re
import matplotlib.pyplot as plt
import veetility
#%%
land_registry_data = pd.read_csv("land_registry_data.csv")
# %%
land_registry_data.columns
#%%

[postcode for postcode in land_registry_data['postcode'].unique() if postcode.startswith("E3 ")]
# %%
[postcode for postcode in land_registry_data['postcode'].unique() if postcode.startswith("E32")]
# %%
postcode = 'E3 2PU'

parts = postcode.split()
area = re.sub(r'[^A-Za-z]', '', parts[0])  # Extracts letters before the space
district = re.sub(r'[^A-Za-z0-9]', '', parts[0])  # Extracts letters and digits before the space

# Check if there's a second part for the postcode
if len(parts) > 1 and parts[1]:
    sector = district + '-' + parts[1][0]  # Adds the first digit of the second part of the postcode
else:
    sector = 'Unknown'

print(area)
print(district)
print(sector)
# %%

# %%
district_groupby = pd.read_csv("District_Prop_Type_Groupby.csv")
# %%
district_groupby = district_groupby[district_groupby['property_type']=='F']

# %%
london_district = district_groupby[district_groupby['is_london?']!='Outside London']
# %%
london_district = london_district[london_district['year'] >= 2021]
# %%
districts_below_30_transactions = london_district[london_district['num_transactions']<=200]['postcode_district'].unique().tolist()
# %%
london_district = london_district[~london_district['postcode_district'].isin(districts_below_30_transactions)]

# %%
plt.scatter(london_district['75th_percentile_price'],london_district['coef_var'])
# %%
