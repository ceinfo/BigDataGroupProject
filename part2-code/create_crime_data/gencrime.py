from __future__ import print_function

import json
import ntpath
import sys
from csv import reader,writer
from itertools import islice
from operator import add
from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth
from shapely.geometry import Point, shape


delim = ","
outputfile = "new_" + ntpath.basename(sys.argv[1])
empty_checkpoint = ["", ""]
unknown_checkpoint = ["UNKNOWN", "UNKNOWN"]
all_boros = ["Brooklyn", "Queens", "Manhattan", "Bronx", "Staten Island"]

found_shape = {}

counter = 0


"""
  checkpoint():  Checks the loaded geojson file and validates
  if found within a neighborhood, returns the data back 
  to the caller.

   long:  longitude of location
   lat:   latitude of location
   Returns:  ntacode, ntaname (ex: ["u'BX14', u'East Concourse-Concourse Village'])
"""
def checkpoint(lon, lat, curboro):

  if long == "" or lat == "":
    return empty_checkpoint

  stdlist = all_boros
  curboro = curboro.title()
  if curboro not in stdlist:
    return unknown_checkpoint
  stdlist.insert(0, stdlist.pop(stdlist.index(curboro)))
  delayedlist = {}

  point = Point(float(lon), float(lat))
  for feature in js["features"]:
    boroname = feature["properties"]["boroname"]
    ntacode = feature["properties"]["ntacode"]
    if boroname == curboro:
      if ntacode in found_shape:
        polygon = found_shape[ntacode]
      else:
        polygon = shape(feature['geometry'])
        if ntacode not in found_shape:
          found_shape[ntacode] = polygon
      if polygon.contains(point):
        properties = feature["properties"]
        data = [properties["ntacode"], properties["ntaname"]]
        return data
    else:
      if boroname not in delayedlist:
        delayedlist[boroname] = []
      delayedlist[boroname].append(feature)

  if len(delayedlist) > 0:
    for boros in stdlist[1:]:
      for feature in delayedlist[boros]:
          properties = feature["properties"]
          boroname = properties["boroname"]
          ntacode = properties["ntacode"]
      
          if ntacode in found_shape:
            polygon = found_shape[ntacode]
          else:
            polygon = shape(feature['geometry'])
          if polygon.contains(point):
            properties = feature["properties"]
            data = [properties["ntacode"], properties["ntaname"]]
            return data
  return unknown_checkpoint
     


###############################################################
#        MAIN
###############################################################

if __name__ == "__main__":
    # Read the geojson file and map to ny neighborhoods
    with open('NTAmap.geojson') as f:
      js = json.load(f)


    # Begin processing with Spark
    sc = SparkContext(appName="FreqItems")
    data = sc.textFile(sys.argv[1])
    transactions = data.mapPartitions(lambda x:  reader(x, 1)) \
        .map(lambda x: (x + checkpoint(x[22], x[21], x[13])))
    results = transactions.collect()


    # Write to the output csv
    with open(outputfile, "wb") as csv_file:
      writer = writer(csv_file, delimiter=',')
      for line in results:
        writer.writerow(line)
        counter += 1
        if counter % 100000 == 0:
          csv_file.flush()
        

    sc.stop()
