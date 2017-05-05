"""
Generate date-ntacode count of crimes
Merge with climate data (if count missing --> 0)

To-do: merge with location demographics
"""


import sys
import os
import string
from csv import reader
from datetime import datetime as dt
import numpy as np

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import Vectors


from pyspark.conf import SparkConf


def formatDate(d):
    d = str(d)
    return d[-4:-2] + '/' + d[-2:] + '/' + d[0:4]

def weekdays(date):
    date = dt.strptime(date, "%m/%d/%Y")
    weekday = date.isoweekday()
    return weekday

def isWeekend(weekday):
    return 1*(weekday >= 6)

def fillNA_AWND(x):
    if x == -9999:
        x = 5.65
    return x

def getYear(date):
    year = str(date)[-4:]
    return year

if __name__ == '__main__':
    sc = SparkContext()
    #spark = SparkSession(sc).builder.master("local").appName("Test").config(conf=SparkConf()).getOrCreate()
    spark = SparkSession(sc).builder.appName("MergeData").config(conf=SparkConf()).getOrCreate()
    sqlContext = SQLContext(sc)
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    #lines = sc.textFile(sys.argv[1], 1)
    df = spark.read.csv(sys.argv[1], sep = '\t', header = True, inferSchema = True, ignoreLeadingWhiteSpace = True, ignoreTrailingWhiteSpace = True)
    df = df.dropna(subset = ('RPT_DT', 'NTACODE'))
    #df1 = df.groupby('RPT_DT').count()
    

    # Process the climate data
    """
    df2 = spark.read.csv(sys.argv[2], sep = ',', header = True, inferSchema = True,ignoreLeadingWhiteSpace = True, ignoreTrailingWhiteSpace = True)    
    df2 = df2.filter((df2.DATE >= 20060101) & (df2.DATE <= 20151231))
    formatDate_udf = udf(formatDate, StringType())
    weekday_udf = udf(weekdays, IntegerType())
    isWeekend_udf = udf(isWeekend, IntegerType())
    fillNA_udf_num = udf(fillNA_AWND, FloatType())
    
    df2 = df2.withColumn('DATE2', formatDate_udf(df2.DATE))
    df2 = df2.select('DATE2', 'PRCP',	'SNWD',	'SNOW', 'TMAX',	'TMIN',	'AWND')
    df2 = df2.withColumn('AWND2', fillNA_udf_num(df2.AWND)).drop('AWND')
    df2 = df2.withColumn('weekday', weekday_udf(df2.DATE2))
    df2 = df2.withColumn('isWeekend', isWeekend_udf(df2.weekday))
    
    
    
    cond1 = [df1.RPT_DT == df2.DATE2]
    df_date = df1.join(df2, cond1, 'right').drop('RPT_DT')
    df_date = df_date.fillna(0, subset = 'count')
    df_date.write.csv('dateCli_cnt.out', sep = '\t', header = True)
        
    df_cor = df_date.drop('DATE2')
    
    df_cor = df_cor.rdd.map(lambda row : Vectors.dense([item for item in row]))
    cor = Statistics.corr(df_cor)
    np.savetxt('cor1.txt', cor, delimiter = '\t', header = "count\tPRCP\tSNWD\tSNOW\tTMAX\tTMIN\tAWND\tweekday\tisWeekend")
    
    # Add location
    
    df1 = df.groupby('RPT_DT', 'NTACODE').count()
    df_loc = df1.select('NTACODE').dropDuplicates()
    df2 = df2.crossJoin(df_loc)
    
    cond2 = [df1.RPT_DT == df2.DATE2, df1.NTACODE == df2.NTACODE]
    df3 = df1.join(df2, cond2, 'right').drop(df1['NTACODE']).drop('RPT_DT')
    df3 = df3.fillna(0, subset = 'count')
    
    print 'Merge complete'
    
    df3 = df3.repartition(1)
    
    print 'Repartitioned'

    df3.write.csv('dateLocCli_cnt.out', sep = '\t', header = True)
    """
    
    # Year-location
    df4 = spark.read.csv('/tmp/newgrp/ce_acs_combined.csv', sep = '\t', header = True, inferSchema = True,ignoreLeadingWhiteSpace = True, ignoreTrailingWhiteSpace = True)    
    
    getYear_udf = udf(getYear, StringType())
    df = df.withColumn('RPT_YR', getYear_udf(df.RPT_DT))
    df1 = df.groupby('RPT_YR', 'NTACODE').count()
    df_loc = df1.select('NTACODE').dropDuplicates()
    df_yr = df1.select('RPT_YR').dropDuplicates()
    
    df4 = df4.join(df_loc, 'NTACODE', 'inner').drop(df_loc['NTACODE'])
    df4 = df4.crossJoin(df_yr)
    
    cond3 = [df4.RPT_YR == df1.RPT_YR, df4.NTACODE == df1.NTACODE]
    df_locYr = df4.join(df1, cond3, 'left').drop(df1['RPT_YR']).drop(df1['NTACODE'])
    df_locYr = df_locYr.fillna(0, subset = 'count')
    df_locYr = df_locYr.withColumn('COUNT_PERCAPITA', df_locYr.count *1.0 / df_locYr.POP_TOT)    
    
    df_cor2 = df_locYr.drop('NTACODE', 'NTANAME')
    lsCol = df_cor2.columns
    df_cor2 = df_cor2.rdd.map(lambda row : Vectors.dense([item for item in row]))
    cor2 = Statistics.corr(df_cor2)
    np.savetxt('cor2.txt', cor2, delimiter = '\t', header = '\t'.join(lsCol))
    print 'Correlation output'
    
    df_locYr.repartition(1).write.csv('locYr_cnt.out', sep = '\t', header = True)
    sc.stop()




