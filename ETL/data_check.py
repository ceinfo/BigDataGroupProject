#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 01:15:53 2017

@author: jshliu

Check the following potential data issues:
    
A. Output records with the following suspicious dates values for manual review:
    1. CMPLNT_TO_DT < CMPLNT_FR_DT
    2. RPT_DT < CMPLNT_FR_DT
    3. CMPLNT_FR_DT empty
    4. CMPLNT_TO_DT - CMPLNT_FR_DT more than 5 days
    5. RPT_DT - CMPLNT_FR_DT more than 5 days

B. Check daily number of crimes and output the top 100 days by crime count
C. Cross-tab of KY_CD and PD_CD
D. Cross-tab of KY_CD and OFNS_DESC

"""

import sys
import os
import string
from csv import reader
from datetime import datetime as dt

from pyspark import SparkContext


def parseData(line):
    """
    Split each line of input data
    """
    entry = []
    line = line.strip()
    for item in reader([line]):
        entry.extend(item)
    try:
        entry[0] = int(entry[0]) # remove header
        # Remove x-y cord, longitude-lattitude for now
        if len(entry) == 24:
            return entry[0:19]
    except:
        pass

def removeNone(entry):
    if entry:
        return True
    else:
        return False


def compareDate(cfd, ctd, rd):
    rule = [False]*5
    days_f2t = ''
    days_f2r = ''
    
    
    if (cfd == '') :
        rule[2] = True
    
    elif cfd != '':
        cfd2 = dt.strptime(cfd, "%m/%d/%Y")
        if ctd != '':
            ctd2 = dt.strptime(ctd, "%m/%d/%Y")
            days_f2t = (ctd2 - cfd2).days
            if days_f2t < 0:
                rule[0] = True
            if days_f2t > 5:
                rule[3] = True
        if rd != '':
            rd2 = dt.strptime(rd, "%m/%d/%Y")
            days_f2r = (rd2 - cfd2).days
            if days_f2r < 0:
                rule[1] = True
            if days_f2r > 5:
                rule[4] = True
    return rule, days_f2t, days_f2r

def addRules(entry):
    rule, days_f2t, days_f2r = compareDate(entry[1], entry[3], entry[5])
    entry.extend(rule)
    entry.extend([days_f2t, days_f2r])
    return entry


def countByDay(entry):
    return (entry[1], 1)

def keyPair(entry, k1, k2, delim):
    return (str(entry[k1]) + delim + str(entry[k2]), 1)

def parseKeyPair(pair, delim):
    Key1, Key2 = pair[0].split(delim, 1)
    return [Key1, Key2, int(pair[1])]

    
if __name__ == '__main__':
    sc = SparkContext()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.map(lambda x: parseData(x)).filter(lambda x: removeNone(x))
    
    # A: Output records with suspicious dates values
    lines_out = lines.map(lambda x: addRules(x)).filter(lambda x: any(x[-7:-2]))
    lines_out = lines_out.map(lambda x: '\t'.join([str(i) for i in x]))
    lines_out.saveAsTextFile('/user/jl7722/crime_dataCheck.out')
    print 'Date check finished'
    
    # B: Daily number of crimes
    cntByDay = lines.map(lambda x: countByDay(x)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], False).take(100)
    sc.parallelize(cntByDay).saveAsTextFile('/user/jl7722/cntByDay.out')
    print 'Daily number calculated'
    
    # C: Cross-tab of KY_CD and PD_CD
    KYPD = lines.map(lambda x: keyPair(x, 6, 8, '\t')).reduceByKey(lambda x,y: x+y, 2).collect()
    KYPD = sc.parallelize(KYPD).map(lambda x: parseKeyPair(x, '\t')).sortBy(lambda x: (x[0], -x[2]))
    KYPD.map(lambda x: '\t'.join([str(i) for i in x])).saveAsTextFile('/user/jl7722/KYPD_tab.out')
    print 'KYPD generated'
    
    # D: Cross-tab of KY_CD and OFNS_DESC
    KYDesp = lines.map(lambda x: keyPair(x, 6, 7, '\t')).reduceByKey(lambda x,y: x+y, 2).collect()
    KYDesp = sc.parallelize(KYDesp).map(lambda x: parseKeyPair(x, '\t')).sortBy(lambda x: (x[0], -x[2]))
    KYDesp.map(lambda x: '\t'.join([str(i) for i in x])).saveAsTextFile('/user/jl7722/KYDesp_tab.out')
    print 'KYDesp generated'
    
    # E: Cross-tab of PD_CD and 
   
   
    sc.stop()
    
    
