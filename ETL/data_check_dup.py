#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 01:15:53 2017

@author: jshliu

Check if the cleaned data has any duplications by the following columns:
    CMPLNT_FR_DT, CMPLNT_FR_DT, CMPLNT_TO_DT, RPT_DT, KY_CD, PD_CD, CRM_ATPT_CPTD_CD, ADDR_PCT_CD

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
    line = line.strip()
    entry = line.split('\t')
    
    if len(entry) == 23:
        return entry
    else:
        pass
    
def parseData2(line):
    """
    Split each line of input data
    """
    entry = []
    line = line.strip()
    for item in reader([line]):
        entry.extend(item)
    return (entry[0], entry[1:])
    
    
    
    
def removeNone(entry):
    if entry:
        return True
    else:
        return False


def getKeys(entry):
    key = entry[1:-5]
    return ('\t'.join(key), entry[0])
    


def getID(entry):
    for i in entry[1]:
        return (str(i), '')

    
if __name__ == '__main__':
    sc = SparkContext()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.map(lambda x: parseData(x)).filter(lambda x: removeNone(x))
    
    # join by key and output duplicates, if any
    lines_out = lines.map(lambda x: getKeys(x)).groupByKey().filter(lambda x: len(x[1]) > 1)
    lines_out = lines_out.map(lambda x: (x[0], [y for y in x[1]]))
    
    lines_out.saveAsTextFile('Dups.out')
    
    """
    lsID = lines_out.flatMap(lambda x: getID(x)).collect()
        
    # output entire records
    if len(lsID) > 0:
        lsID = sc.parallelize(lsID)
        lines2 = sc.textFile(sys.argv[2], 1).map(lambda x: parseData2(x))
        lines2 = lines2.join(lsID).collect()
        
        sc.parallelize(lines2).saveAsTextFile('DupsRecords.out')
    """
    
    sc.stop()
    
    