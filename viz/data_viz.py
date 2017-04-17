#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 01:15:53 2017

@author: jshliu

Output summary stats for visualization:
    1. Count by type - year
    2. Count by report - from date
    3. Count by to - from date
    4. Count by from month
    
"""

import sys
import os
import string
from csv import reader
from pyspark import SparkContext
from datetime import datetime as dt


dictKYCD = {'101': 'Murder', '104': 'Rape', '105': 'Robbery', '107': 'Burglary', 
           '117': 'Drugs', '118': 'Weapons'}


def mapKYCD(cd, mapDict):
    if cd in mapDict:
        return mapDict[cd]
    else:
        return 'Others'


def getMonth(text):
    if text != '':
        mm, dd, year = text.split('/',2)
        return mm
    else:
        return ''

def getYear(text):
    if text != '':
        mm, dd, year = text.split('/',2)
        return year
    else:
        return ''
    

def getDifDays(cfd, ctd, rd):
    days_f2t = -1
    days_f2r = -1
    
    if cfd != '' :  
        cfd2 = dt.strptime(cfd, "%m/%d/%Y")
        if ctd != '':
            ctd2 = dt.strptime(ctd, "%m/%d/%Y")
            days_f2t = (ctd2 - cfd2).days
        if rd != '':
            rd2 = dt.strptime(rd, "%m/%d/%Y")
            days_f2r = (rd2 - cfd2).days
    return days_f2t, days_f2r
            

def parseData(line):
    """
    Split each line of input data
    """
    line = line.strip()
    entry = line.split('\t')
    
    if len(entry) == 23:
        try:            
            days_f2t, days_f2r = getDifDays(entry[1], entry[3], entry[5])
            fromMonth = getMonth(entry[1])
            reportYear = getYear(entry[5])
            KY = mapKYCD(entry[6], dictKYCD)
            entry.extend([days_f2t, days_f2r, fromMonth, reportYear, KY])
            return entry
        except:
            pass
    

def removeNone(entry):
    if entry:
        return True
    else:
        return False

    
def countByKey(entry, key):
    return (entry[key], 1)

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

    # Count by type - year
    Type = lines.map(lambda x: keyPair(x, -1, -2, '\t')).reduceByKey(lambda x,y: x+y).collect()
    Type = sc.parallelize(Type).map(lambda x: parseKeyPair(x, '\t')).sortBy(lambda x: (x[0], x[1]))
    Type.map(lambda x: '\t'.join([str(i) for i in x])).saveAsTextFile('TypeYear.out')
    print 'Type generated'

    # Count by to - from date:
    cntByDay_f2t = lines.map(lambda x: countByKey(x, -5)).reduceByKey(lambda x,y: x+y).collect()
    sc.parallelize(cntByDay_f2t).map(lambda x: '\t'.join([str(i) for i in x])).saveAsTextFile('cntByDay_f2t.out')
    print 'f2t calculated'
   
    # Count by report - from date:
    cntByDay_f2r = lines.map(lambda x: countByKey(x, -4)).reduceByKey(lambda x,y: x+y).collect()
    sc.parallelize(cntByDay_f2r).map(lambda x: '\t'.join([str(i) for i in x])).saveAsTextFile('cntByDay_f2r.out')
    print 'f2r calculated'

    # Count by from month:
    cntByMonth = lines.map(lambda x: countByKey(x, -3)).reduceByKey(lambda x,y: x+y).collect()
    sc.parallelize(cntByMonth).map(lambda x: '\t'.join([str(i) for i in x])).saveAsTextFile('cntByMonth.out')
    print 'month calculated'
        
    sc.stop()




















    
        
                    
    
    
    
    
    
    
    
    
    
    
    