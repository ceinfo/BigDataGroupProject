#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 01:15:53 2017

@author: jshliu

Output a cleaned version of the data set:
    1. Remove rows without CMPLNT_FR_DT and CMPLNT_TO_DT
    2. Update rows without CMPLNT_FR_DT but has CMPLNT_TO_DT
    3. Map CRM_ATPT_CPTD_CD, LAW_CAT_CD and BORO_NM to codes
    4. Only keep hours in CMPLNT_FR_TM and CMPLNT_TO_TM
    
"""

import sys
import os
import string
from csv import reader
from pyspark import SparkContext

mapCRM = {'ATTEMPTED': 1, 
          'COMPLETED': 2, 
          '': 0}

mapLAW = {'': 0,
          'FELONY':3, 
          'MISDEMEANOR': 2, 
          'VIOLATION': 1}

mapBORO = {'': 0,
           'BRONX': 1, 
           'BROOKLYN': 2, 
           'MANHATTAN': 3, 
           'QUEENS': 4, 
           'STATEN ISLAND':5}

def parseData(line):
    """
    Split each line of input data
    """
    entry = []
    line = line.strip()
    for item in reader([line]):
        entry.extend(item)
    # Remove longitude-lattitude for now
    return entry[0:21]

def text2Code(text, codeMap, nonExistCode = 99):
    text = text.strip()
    if text in codeMap:
        return codeMap[text]
    else:
        return nonExistCode


def getHours(text):
    if text != '':
        return text.split(':',2)[0]
    
    