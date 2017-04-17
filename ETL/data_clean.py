#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 01:15:53 2017

@author: jshliu

Output a cleaned version of the data set:
    
"""

import sys
import os
import string
from csv import reader
from pyspark import SparkContext
from datetime import datetime as dt


def updateKYCD(code, desp):
    if code == '120':
        if desp == 'CHILD ABANDONMENT/NON SUPPORT':
            out = '120.1'
        else:
            out = '120.2'
    elif code == '343':
        if desp == 'OTHER OFFENSES RELATED TO THEF':
            out = '343.1'
        else:
            out = '343.2'
    elif code == '345':
        if desp == 'OFFENSES RELATED TO CHILDREN':
            out = '345.1'
        else:
            out = '345.2'
    elif code == '364':
        if desp == 'AGRICULTURE & MRKTS LAW-UNCLASSIFIED':
            out = '364.2'
        else:
            out = '364.1'
    elif code == '677':
        if desp == 'OTHER STATE LAWS':
            out = '677.1'
        else:
            out = '677.2'
    else:
        out = code
        return out
        

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
            return entry[0:23]
    except:
        pass


def getHours(text):
    if text != '':
        return text.split(':',2)[0]
    else:
        return ''
    

def updateYear(date):
    year = int(date[-4:])
    year_2dig = int(date[-2:])
    day = date[0:-4]
    flg_update = False
    if (year < 1917) and (year_2dig < 17):
        date = date[0:-4] + '20' + date[-2:]
        flg_update = True
    return date, flg_update, year, day


def fixDates_part1(date):
    checkValue = 0
    if date == '':
        checkValue = 1
        year = 0
        day = ''
    else:
        date, flg_update, year, day = updateYear(date)
        if flg_update:
            checkValue = 2    
    return date, checkValue, year, day


def fixDates_part2(from_date, to_date, report_date):
    
    # Fix missings and value before 1917
    from_date, from_date_value, from_year, from_day = fixDates_part1(from_date)
    to_date, to_date_value, to_year, to_day = fixDates_part1(to_date)
    report_date, report_date_value, report_year, report_day = fixDates_part1(report_date)
    
    # Check year of from_date
    if (from_year >= 1917) & (from_year < 1957):
        if (from_day == to_day) & (to_year >= 2000):
            from_date = to_date
            from_date_value = 2
        elif (from_day == report_day) & (report_year >= 2000):
            from_date = report_date
            from_date_value = 2
        else:
            from_date_value = 3
    
    # Check if report_date < from_date
    if (from_date != ''):
        from_date2 = dt.strptime(from_date, "%m/%d/%Y")
        report_date2 = dt.strptime(report_date, "%m/%d/%Y")
        if report_date2 < from_date2:
            report_date = ''
            report_date_value = 3
        
    # Check if to_date < from_date        
        if (to_date != ''):
            to_date2 = dt.strptime(to_date, "%m/%d/%Y")    
            if to_date2 < from_date2:
                if (from_date2 - to_date2).days <= 360:
                    from_date, to_date = to_date, from_date
                    from_date_value = 2
                    to_date_value = 2
                elif (from_day == to_day) & (str(from_year)[-1] == str(to_year)[-1]):
                    to_date = to_day + str(from_year)
                    to_date_value = 2
                else:
                    from_date, to_date = '', ''
                    from_date_value, to_date_value = 3,3
    
    return from_date, to_date, report_date, from_date_value, to_date_value, report_date_value

def removeNone(entry):
    if entry:
        return True
    else:
        return False


def getCleanEntry(entry):
    entry_new = [entry[0]]
    
    from_date, to_date, report_date, from_date_value, to_date_value, report_date_value = fixDates_part2(entry[1], entry[3], entry[5])
    
    fr_tm = getHours(entry[2])
    to_tm = getHours(entry[4])
    
    KY_CD = updateKYCD(entry[6], entry[7])
    
    entry_new.extend([from_date, fr_tm, to_date, to_tm, report_date, KY_CD, entry[8]])
    entry_new.extend(entry[10:15])
    entry_new.extend(entry[16:])
    entry_new.extend([from_date_value, to_date_value, report_date_value])
    line_new = '\t'.join([str(i) for i in entry_new])
    return line_new
    


if __name__ == '__main__':
    sc = SparkContext()
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.map(lambda x: parseData(x)).filter(lambda x: removeNone(x))
    lines_new = lines.map(lambda x: getCleanEntry(x))
    
    header = sc.parallelize(['CMPLNT_NUM\tCMPLNT_FR_DT\tCMPLNT_FR_TM\tCMPLNT_TO_DT\tCMPLNT_TO_TM\tRPT_DT\tKY_CD\tPD_CD\t\
                             CRM_ATPT_CPTD_CD\tLAW_CAT_CD\tJURIS_DESC\tBORO_NM\tADDR_PCT_CD\tPREM_TYP_DESC\tPARKS_NM\t\
                             HADEVELOPT\tX_COORD_CD\tY_COORD_CD\tLatitude\tLongitude\tFROM_DT_value\tTO_DT_value\tRPT_DT_value'])
    header.union(lines_new).saveAsTextFile('NYPD_clean.out')
    
    sc.stop()




















    
        
                    
    
    
    
    
    
    
    
    
    
    
    