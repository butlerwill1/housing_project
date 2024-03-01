import pandas as pd
#%% ---------------------------------------------------------------------------------------------------
#                                   Functions for Pandas operations
# -----------------------------------------------------------------------------------------------------

def postcode_socio_grouby_agg(x):

    d = {}
    d['geometry'] = x['postcode_dist_geometry'].iloc[0]
    d['AreaName'] = x['LADnm'].iloc[0]
    d['Locale'] = x['Locale'].iloc[0]
    d['CountLowLevelAreas'] = x.shape[0]
    d['AreaKm2'] = x['AreaKm2'].iloc[0]
    d['Population'] = x['TotPop'].sum()
    d['Population_16-59'] = x['Pop16_59'].sum()
    d['Population_60+'] = x['Pop60+'].sum()
    d['PopulationDensity'] = round(d['Population'] / d['AreaKm2'], 2)

    # Weighted Score of multiple factors, weightings can be found in "English_Socio_Economic.md"   
    d['OverallAvg'] = round(x['IMDScore'].mean(), 2)
    d['OverallRankAvg'] = round(x['IMDRank0'].mean(), 2)
    d['OverallMedian'] = round(x['IMDScore'].median(), 2)
    d['OverallMax'] = x['IMDScore'].max()
    d['OverallMin'] = x['IMDScore'].min()

    # Income Score
    d['IncomeAvg'] = round(x['IncScore'].mean(), 2)
    d['InccomeRankAvg'] = round(x['IncRank'].mean(), 2)
    d['IncomeMedian'] = round(x['IncScore'].median(), 2)
    d['IncomeMax'] = x['IncScore'].max()
    d['IncomeMin'] = x['IncScore'].min()

    # Education, skills and training
    d['EducationAvg'] = round(x['EduScore'].mean(), 2)
    d['EducationRankAvg'] = round(x['EduRank'].mean(), 2)
    d['EducationMedian'] = round(x['EduScore'].median(), 2)
    d['EducationMax'] = x['EduScore'].max()
    d['EducationMin'] = x['EduScore'].min()

    # Crime Score
    d['CrimeAvg'] = round(x['CriScore'].mean(), 2)
    d['CrimeRankAvg'] = round(x['CriRank'].mean(), 2)
    d['CrimeMedian'] = round(x['CriScore'].median(), 2)
    d['CrimeMax'] = x['CriScore'].max()
    d['CrimeMin'] = x['CriScore'].min()
    
    # Environment Score, measures the quality of the local environment
    d['EnvironmentAvg'] = round(x['EnvScore'].mean(), 2)
    d['EnvironmentRankAvg'] = round(x['EnvRank'].mean(), 2)
    d['EnvironmentMedian'] = round(x['EnvScore'].median(), 2)
    d['EnvironmentMax'] = x['EnvScore'].max()
    d['EnvironmentMin'] = x['EnvScore'].min()

    # Geographical Barriers Score, physical proximity to local services
    d['GeographBarriersAvg'] = round(x['GBScore'].mean(), 2)
    d['GeographBarriersRankAvg'] = round(x['GBRank'].mean(), 2)
    d['GeographBarriersMedian'] = round(x['GBScore'].median(), 2)
    d['GeographBarriersMax'] = x['GBScore'].max()
    d['GeographBarriersMin'] = x['GBScore'].min()

    # Indoor Living Environment Score
    d['IndoorLivingAvg'] = round(x['IndScore'].mean(), 2)
    d['IndoorLivingRankAvg'] = round(x['IndRank'].mean(), 2)
    d['IndoorLivingMedian'] = round(x['IndScore'].median(), 2)
    d['IndoorLivingMax'] = x['IndScore'].max()
    d['IndoorLivingMin'] = x['IndScore'].min()

    # Childrens and Young People's service
    d['YoungServicesAvg'] = round(x['CYPScore'].mean(), 2)
    d['YoungServicesAvg'] = round(x['CYPRank'].mean(), 2)
    d['YoungServicesMedian'] = round(x['CYPScore'].median(), 2)
    d['YoungServicesMax'] = x['CYPScore'].max()
    d['YoungServicesMin'] = x['CYPScore'].min()

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
