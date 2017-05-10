#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 01:15:53 2017

@author: jshliu

Output graphs:
    1. Count by type - year
    2. Count by report - from date
    3. Count by to - from date
    4. Count by from month
    
"""

import sys
import os
import string
from datetime import datetime as dt
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.gridspec as gridspec

import plotly.plotly as py
import plotly.graph_objs as go



os.chdir('jshliu/Desktop/project')

def loaddata(name, colnames):
    df = pd.read_csv(name, sep = '\t', header=None, names = colnames)
    return df

TypeYear = loaddata('TypeYear.txt', colnames = ['Crime_type', 'Report_year', 'Count'])
TypeYear = TypeYear[pd.notnull(TypeYear['Report_year'])]
TypeYear['Report_year'] = TypeYear['Report_year'].map(int)

f2t = loaddata('f2t.txt', colnames = ['Days_f2t', 'Count'])
f2r = loaddata('f2r.txt', colnames = ['Days_f2r', 'Count'])
cntByMonth = loaddata('cntByMonth.txt', colnames = ['From_month', 'Count'])


def typePlot(ax, typeName):
    df = TypeYear[TypeYear['Crime_type']== typeName]
    ax.plot(df['Report_year'], df['Count'], 'b-')
    ax.set_xlim([2006, 2015])
    ax.set_ylim([0, max(df['Count'] + 1)])
    ax.set_ylabel('Number of reported crimes')
    ax.set_xlabel('Report year')
    ax.set_xticklabels(range(2006, 2015), rotation=20)
    ax.set_title(typeName)
    return ax

lstype = ['Burglary', 'Drugs', 'Murder', 'Rape', 'Robbery', 'Weapons']


pp = PdfPages('Type_plot.pdf')  
figure1 = plt.figure(figsize=(15, 10))
gs1 = gridspec.GridSpec(2, 3)
for i in range(6):
    ax = figure1.add_subplot(gs1[i])
    ax = typePlot(ax, lstype[i])
figure1.suptitle('Number of specific types of crime by report year', fontsize=12)
pp.savefig(figure1)
plt.close(figure1)
    
pp.close()



def groupDays(day):
    if day < 3:
        out = day
    elif day < 7:
        out = 3
    elif day < 30:
        out = 4
    elif day < 90:
        out = 5
    elif day < 365:
        out = 6
    elif day < 365*5:
        out = 7
    elif day < 3650:
        out = 8
    else:
        out = 9
    return out

lsLabel = ['Misisng', 'Same day', '1 day', '2 days', '3-7 days', '1 month', '3 months', 
           '1 year', '5 years', '10 years', '> 10 years']

f2t['days_grp'] = f2t['Days_f2t'].map(groupDays)
f2t2 = f2t.groupby('days_grp')['Count'].sum().reset_index()

f2r['days_grp'] = f2r['Days_f2r'].map(groupDays)
f2r2 = f2r.groupby('days_grp')['Count'].sum().reset_index()

pp = PdfPages('Days_plot.pdf')  

figure2 = plt.figure(figsize=(12, 6))
ax = figure2.add_subplot(111)
ax.bar(f2t2['days_grp'], f2t2['Count'], color='b')
ax.set_xlim([-1.2, 10.2])
ax.set_ylabel('Number of reported crimes', fontsize=14)
ax.set_xlabel('Crime duration (days between from date and to date)', fontsize=14)
ax.set_xticks(f2t2['days_grp'])
ax.set_xticklabels(lsLabel, rotation = 30, fontsize=14)
ax.set_title('Number of crimes by duration', fontsize=18)

pp.savefig(figure2)
plt.close(figure2)

figure3 = plt.figure(figsize=(12, 6))
ax = figure3.add_subplot(111)
ax.bar(f2r2['days_grp'], f2r2['Count'], color='b')
ax.set_xlim([-1.2, 10.2])
ax.set_ylabel('Number of reported crimes', fontsize=14)
ax.set_xlabel('Crime report lag (days between from date and report date)', fontsize=14)
ax.set_xticks(f2r2['days_grp'])
ax.set_xticklabels(lsLabel, rotation = 30, fontsize=14)
ax.set_title('Number of crimes by reporting lag', fontsize=18)

pp.savefig(figure3)
plt.close(figure3)

pp.close()


#============ Summary of type and location ==============

TypeCnt = loaddata('countType.txt', colnames = ['Count', 'KY_CD'])
LocCnt = loaddata('countLoc.txt', colnames = ['Count', 'Location'])


#============ Plot by dates =================
cntDate = loaddata('cntByDate.txt', colnames = ['Year', 'Date', 'Count'])
cntDate = cntDate[pd.notnull(cntDate['Year'])]
cntDate['MMDD'] = cntDate['Date'].map(lambda x: x[0:-5])


lsDate = list(set(cntDate['MMDD']))
lsDate.sort()

dfDate = pd.DataFrame({'MMDD': lsDate, 'order': np.arange(len(lsDate)), 'temp': np.ones(len(lsDate)) })
dfYear = pd.DataFrame({'Year': np.arange(2006, 2016), 'temp': np.ones(10)})
dfDate2 = pd.merge(dfYear, dfDate, how = 'outer', on = 'temp')
dfDate2 = dfDate2.drop('temp', axis = 1)

cntDate2 = pd.merge(cntDate, dfDate2, how = 'right', on = ['Year', 'MMDD'])
labels = [lsDate[i*15] for i in range(366/15)]

data = []

for year in range(2006, 2016):
    df = cntDate2[cntDate2['Year'] == year].sort_values('order')
    trace = go.Scatter(
    x = df['order'],
    y = df['Count'],
    text = lsDate,
    mode = 'lines',
    name = str(year),
    #marker = dict(opacity = 0.5, size = 4)
    line=dict(width=1),
    
    )
    data.append(trace)

layout = dict(title = 'Number of Crimes by Report Date',
              xaxis = dict(title = 'Dates', 
                           ticktext=labels,
                           tickvals=[i * 15 for i in range(366/15)]
                           ),
              yaxis = dict(title = 'Number of reported crimes'),
              autosize=False,
              width=900,
              height=500,
              )

fig = dict(data=data, layout=layout)
py.iplot(fig, filename='BD/cntByDate')


#=========== Correlations and scatter plots ================
dateCli = pd.read_csv('dateCli_cnt.txt', sep = '\t')
locYr = pd.read_csv('locYr_cnt.txt', sep = '\t')

check = locYr[locYr['count'] > 10000]

from scipy.stats import pearsonr, ttest_ind

pearsonr(locYr['count'], locYr['POP_TOT'])

def getCor(df, lsIdx):
    lsCol = df.columns
    cor_value = []
    p_value = []
    names = [lsCol[j] for j in lsIdx]
    for var in names:
        cor = pearsonr(df['count'], df[var])
        cor_value.append(cor[0])
        p_value.append(cor[1])
    
    dfCor = pd.DataFrame({'vars': names, 'cor': cor_value, 'p_value': p_value})
    return dfCor
    
dfCor_loc = getCor(locYr, range(3, 37))
dfCor_cli = getCor(dateCli, range(2, 10))


# Scatter plots
lsVar = ['PRCP', 'SNWD', 'TMAX', 'AWND2']
figure1 = plt.figure(figsize=(12, 12))
gs1 = gridspec.GridSpec(2, 2)
for i in range(len(lsVar)):
    ax = figure1.add_subplot(gs1[i])
    ax.scatter(dateCli[lsVar[i]], dateCli['count'])
    
    ax.set_ylabel('Number of reported crimes', fontsize = 16)
    ax.set_xlabel(lsVar[i], fontsize = 16)
    

lsVar = ['POP_TOT', 'RACE_WHITE_RT', 'POP_OVER60YR_RT', 'HOUSEHOLD_LT25K_RT',
         'HOUSEUNITS_OWNER_RT', 'EDU_LT12_RT']

figure1 = plt.figure(figsize=(18, 12))
gs1 = gridspec.GridSpec(2, 3)
for i in range(len(lsVar)):
    ax = figure1.add_subplot(gs1[i])
    ax.scatter(locYr[lsVar[i]], locYr['count'])
    
    ax.set_ylabel('Number of reported crimes', fontsize = 16)
    ax.set_xlabel(lsVar[i], fontsize = 16)


# Boxplot
dt = [np.array(dateCli['count'][dateCli['isWeekend'] == 0]), np.array(dateCli['count'][dateCli['isWeekend'] == 1])]

fig = plt.figure(figsize = (6,4))
ax = fig.add_subplot(111)
ax.boxplot(dt, labels = ['Weekdays', 'Weekends'], widths = (0.5,0.5))
ax.tick_params(labelsize=12)
ax.set_ylabel('Number of reported crimes', fontsize = 14)


ttest_ind(np.array(dateCli['count'][dateCli['isWeekend'] == 0]), np.array(dateCli['count'][dateCli['isWeekend'] == 1]))



