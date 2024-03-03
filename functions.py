import pandas as pd
#%% ---------------------------------------------------------------------------------------------------
#                                   Functions for Pandas operations
# -----------------------------------------------------------------------------------------------------
indicators_to_aggregate = ['Overall', 'Income', 'Education', 
                           'Crime', 'Environment', 'GeographicalBarriers',
                           'IndoorLiving']

def postcode_socio_grouby_agg(x):

    d = {}
    d['geometry'] = x['postcode_dist_geometry'].iloc[0]
    d['AreaName'] = x['LADnm'].iloc[0]
    d['Locale'] = x['Locale'].iloc[0]
    d['CountLowLevelAreas'] = x.shape[0]
    d['AreaKm2'] = x['AreaKm2'].iloc[0]
    d['Population'] = x['TotPop'].sum()
    d['Population16-59%'] = round(x['Pop16_59'].sum() * 100 / d['Population'], 2)
    d['Population60+%'] = round(x['Pop60+'].sum() * 100 / d['Population'], 2)
    d['PopulationDensity'] = round(d['Population'] / d['AreaKm2'], 2)

    # Weighted Score of multiple factors, weightings can be found in "English_Socio_Economic.md

    for indicator in indicators_to_aggregate:
        d[f'{indicator}Avg'] = round(x[f'{indicator}Score'].mean(),2)
        d[f'{indicator}RankAvg'] = round(x[f'{indicator}Rank'].mean(),2)
        d[f'{indicator}Median'] = round(x[f'{indicator}Score'].median(),2)
        d[f'{indicator}Min'] = round(x[f'{indicator}Score'].min(),2)
        d[f'{indicator}Max'] = round(x[f'{indicator}Score'].max(),2)


    return pd.Series(d, index=list(d.keys()))

def clean_socio_columns(column_name):
    
    replacements = {
        'IMD' : 'Overall',
        'Inc' : 'Income',
        'Emp' : 'Employment',
        'HDD' : 'Health',
        'Cri' : 'Crime',
        'BHS' : 'HousingBarriers',
        'Env' : 'Environment',
        'Edu' : 'Education',
        'IDC' : 'IncomeDeprChildren',
        'IDO' : 'IncomeDeprOlder',
        'CYP' : 'Services4Young',
        'AS' : 'AdultSkills',
        'GB' : 'GeographicalBarriers',
        'WB' : 'WiderBarriers',
        'Ind' : 'IndoorLiving',
        'Out' : 'OutdoorLiving'
    }
    for key, value in replacements.items():
        column_name = column_name.replace(key, value)
    
    return column_name
