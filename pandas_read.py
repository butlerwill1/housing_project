#%%----------------------------------------------------------------------------------------------------
#                                       Pandas test area
#%%----------------------------------------------------------------------------------------------------

import pandas as pd
import regex as re
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
