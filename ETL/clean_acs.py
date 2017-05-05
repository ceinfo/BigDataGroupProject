"""
Further clean the acs data and subset to usable columns
"""

import pandas as pd
import numpy as np
import os
import pdb

os.chdir('./jshliu/Desktop/project/')

# Remove commas in input file
os.system("./cleaned_acs/rmComma.sh")

def keepRenameCol(df, colIdx, colName):
    lsCol = np.array(df.columns)
    keep = list(lsCol[colIdx])
    df = df[keep]
    dictRename = {}
    for i in range(len(keep)):
        dictRename[keep[i]] = colName[i]
    df = df.rename(columns = dictRename)
    return df

def getRatio(df, denomIdx, numIdx, drop = True):
    lsCol = list(df.columns)
    name = [lsCol[i] + '_RT'  for i in numIdx]
    for i in range(len(numIdx)):        
        df[name[i]] = df[lsCol[numIdx[i]]]*1.0 / (df[lsCol[denomIdx]] + 1e-8)
    if drop:
        df.drop([lsCol[i] for i in numIdx], axis = 1, inplace = True)
    return df

def getAve(df, sumIdx, lsMultiplier, name, drop = True):
    lsCol = list(df.columns)
    df[name + '_SUM'] = 0
    df[name + '_CNT'] = 0
    for i in range(len(sumIdx)):
        df[name + '_SUM'] = df[name + '_SUM'] + df[lsCol[sumIdx[i]]] * lsMultiplier[i]
        df[name + '_CNT'] = df[name + '_CNT'] + df[lsCol[sumIdx[i]]] 
        
    df[name + '_AVE'] = df[name + '_SUM'] * 1.0 / (df[name + '_CNT'] + 1e-8)
    if drop:
        df.drop([lsCol[idx] for idx in sumIdx], axis = 1, inplace = True)
    df.drop([name + '_SUM', name + '_CNT'], axis = 1, inplace = True)
    return df
        
    
    
    
# 1. Demographics:
    
dfDem = pd.read_csv('cleaned_acs/ce_acs_demo_08to12_ntas.csv', sep = '|')
dfDem = dfDem[0:-1] # Remove cemetry

colIdx = [1,2]
colIdx.extend(range(4,20))
colIdx.extend([71,77,78,80])

colName = ['NTACODE', 'NTANAME', 'POP_TOT', 'POP_MALE', 'POP_FEMALE', 'POP_UNDER5YR', 
           'POP_5TO9YR', 'POP_10TO14YR', 'POP_15TO19YR', 'POP_20TO24YR', 'POP_25TO34YR', 
           'POP_35TO44YR', 'POP_45TO54YR', 'POP_55TO59YR', 'POP_60TO64YR', 'POP_65TO74YR',
           'POP_75TO84YR', 'POP_OVER85YR', 'RACE_HIS_LAT', 'RACE_WHITE', 'RACE_BLACK', 'RACE_ASIAN']

dfDem = keepRenameCol(dfDem, colIdx, colName)
dfDem['POP_UNDER10YR'] = dfDem['POP_UNDER5YR'] + dfDem['POP_5TO9YR']
dfDem['POP_10TO19YR'] = dfDem['POP_10TO14YR'] + dfDem['POP_15TO19YR']
dfDem['POP_20TO59YR'] = dfDem['POP_20TO24YR'] + dfDem['POP_25TO34YR'] + dfDem['POP_35TO44YR'] + dfDem['POP_45TO54YR'] + dfDem['POP_55TO59YR']
dfDem['POP_OVER60YR'] = dfDem['POP_60TO64YR'] + dfDem['POP_65TO74YR'] + dfDem['POP_75TO84YR'] + dfDem['POP_OVER85YR'] 
dfDem.drop(['POP_UNDER5YR', 'POP_5TO9YR', 'POP_10TO14YR', 'POP_15TO19YR', 'POP_20TO24YR', 'POP_25TO34YR', 
           'POP_35TO44YR', 'POP_45TO54YR', 'POP_55TO59YR', 'POP_60TO64YR', 'POP_65TO74YR',
           'POP_75TO84YR', 'POP_OVER85YR'], axis=1, inplace=True)


dfDem = getRatio(dfDem, 2, range(3,13))

    
# 2. Econ
dfEco = pd.read_csv('cleaned_acs/ce_acs_select_econ_08to12_ntas.csv', sep = '|')
dfEco = dfEco[0:-1]

colIdx = [1,6,7,8,50,51,52,53,54,55,56,57,58,59,60,61,66]
colName = ['NTACODE', 'LABOR_FORCE', 'EMPLOYED', 'UNEMPLOYED', 'HOUSEHOLD_TOT', 'HOUSEHOLD_LT10K', 'HOUSEHOLD_10KTO15K', 
           'HOUSEHOLD_15KTO25K', 'HOUSEHOLD_25KTO35K', 'HOUSEHOLD_35KTO50K', 'HOUSEHOLD_50KTO75K', 'HOUSEHOLD_75KTO100K',
           'HOUSEHOLD_100KTO150K', 'HOUSEHOLD_150KTO200K', 'HOUSEHOLD_OVER200K', 'HOUSEHOLD_WITHEARNING', 'HOUSEHOLD_FOODSTAMP']

dfEco = keepRenameCol(dfEco, colIdx, colName)
dfEco['HOUSEHOLD_LT25K'] = dfEco['HOUSEHOLD_LT10K'] + dfEco['HOUSEHOLD_10KTO15K'] + dfEco['HOUSEHOLD_15KTO25K']
dfEco['HOUSEHOLD_25KTO50K'] = dfEco['HOUSEHOLD_25KTO35K'] + dfEco['HOUSEHOLD_35KTO50K']
dfEco['HOUSEHOLD_50KTO200K'] = dfEco['HOUSEHOLD_50KTO75K']+ dfEco['HOUSEHOLD_75KTO100K']+ dfEco['HOUSEHOLD_100KTO150K']+ dfEco['HOUSEHOLD_150KTO200K']


sumIdx = range(5,15)
lsMultiplier = [10, 12.5, 20, 30, 42.5, 62.5, 87.5, 125, 175, 225]
dfEco = getAve(dfEco, sumIdx, lsMultiplier, 'HOUSEHOLD_INCOME')
dfEco = getRatio(dfEco, 1, range(2,4))
dfEco = getRatio(dfEco, 2, range(3,8))


# 3. Housing
dfHouse = pd.read_csv('cleaned_acs/ce_acs_select_housing_08to12_ntas.csv', sep = '|')
dfHouse = dfHouse[0:-1]
colIdx = [1,4,5,6,50,51,79,80,81]
colName = ['NTACODE', 'HOUSEUNITS_TOT', 'HOUSEUNITS_OCCUPIED', 'HOUSEUNITS_VACANT', 'HOUSEUNITS_OWNER', 'HOUSEUNITS_RENTER', 
           'HOUSEUNITS_NOPLUMBING', 'HOUSEUNITS_NOKITCHEN', 'HOUSEUNITS_NOPHONE']

dfHouse = keepRenameCol(dfHouse, colIdx, colName)
dfHouse = getRatio(dfHouse, 1, [3,6,7,8])
dfHouse = getRatio(dfHouse, 2, [3,4])
#Maybe: average building years, average rent


# 4. social
dfSoc = pd.read_csv('cleaned_acs/ce_acs_socio_08to12_ntas.csv', sep = '|')
dfSoc = dfSoc[0:-1]

colIdx = [1]
colIdx.extend(range(59, 67))
colName = ['NTACODE', 'POP_OVER25YR', 'EDU_LT9', 'EDU_9TO12', 'EDU_HIGHSCHOOL', 'EDU_NONDEGREE',
           'EDU_ASSOC', 'EDU_BACHELOR', 'EDU_GRADUATE']
dfSoc = keepRenameCol(dfSoc, colIdx, colName)
dfSoc['EDU_LT12'] = dfSoc['EDU_LT9'].map(float) + dfSoc['EDU_9TO12'].map(float)
dfSoc['EDU_PRIORCOLLEGE'] = dfSoc['EDU_HIGHSCHOOL'].map(float) + dfSoc['EDU_NONDEGREE'].map(float) + dfSoc['EDU_ASSOC'].map(float)
dfSoc['EDU_COLLEGE'] = dfSoc['EDU_BACHELOR'].map(float) + dfSoc['EDU_GRADUATE'].map(float)
dfSoc.drop(['EDU_LT9', 'EDU_9TO12', 'EDU_HIGHSCHOOL', 'EDU_NONDEGREE',
           'EDU_ASSOC', 'EDU_BACHELOR', 'EDU_GRADUATE'], axis=1, inplace=True)

dfSoc['POP_OVER25YR'] = dfSoc['POP_OVER25YR'].map(int)
dfSoc = getRatio(dfSoc, 1, range(2,5))

# merge
dfDem = pd.merge(dfDem, dfEco, on = 'NTACODE')
dfDem = pd.merge(dfDem, dfHouse, on = 'NTACODE')
dfDem = pd.merge(dfDem, dfSoc, on = 'NTACODE')

dfDem['POP_PERHOUSEHOLD'] = dfDem['POP_TOT'] * 1.0 / (dfDem['HOUSEUNITS_TOT'] + 1e-8)

dfDem.to_csv('cleaned_acs/ce_acs_combined.csv',sep = '\t', index = False)







