#!/usr/bin/python

"""
  Author:  Catherine Eng
  This code is used to analyze the NYC Crime Data file.  Will parse through and pick out anomalies, 
  some interesting pieces of the data in our research.
"""

import ast
import csv
import re
import sys
from collections import OrderedDict
from datetime import datetime

counter = {}
semantic = {}
valid = {}

MAX_INT = 2147483647
STRING_MARGIN = 10
DECIMAL = "decimal"
INTEGER = "integer"
LONG = "long"
STRING = "string"
DATETIME = "datetime"
DATE = "date"
TIME = "time"
list_datatypes = [DECIMAL, INTEGER, LONG, DATETIME]

EMAIL = "email"
PHONE = "phone"
ADDRESS = "address"
ZIPCODE = "zipcode"
STATE = "state"

EMPTY = "empty"
ZERO = "zero"
VALUE = "value"
NULL = "null"
VALID = "valid"
INVALID = "invalid"


date_formats = {
    # Assume American Date Format
    "%m/%d/%Y %I:%M %p": DATETIME,
    "%m/%d/%Y %I:%M:%S %p": DATETIME,
    "%m/%d/%Y %H:%M:%S": DATETIME,
    "%m/%d/%Y %H:%M": DATETIME,
    "%m/%d/%Y": DATE,
    "%H:%M:%S": TIME,
    "%H:%M": TIME,
}

semantic_formats = { 
    "^[(\d{3})]*[-]*\d{3}-\d{4}":  PHONE,
    "^\d{5}-\d{4}$":  ZIPCODE,
    "^\d{5}$":  ZIPCODE,
    "^\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)":  ADDRESS,
    "(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)":  EMAIL,
}

states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}



def check_datetime(value):
  for x in date_formats:
    try:
      datetime.strptime(value, x)
      return date_formats[x] 
    except ValueError:
      continue



def check_datatype(value, inindex):
  index = str(inindex)
  if index not in counter:
    counter[index] = {}

  for elem in list_datatypes:
    x = str(elem)
    if x not in counter[index]:
      counter[index][x] = 0

  try:
    # Is formatted as a decimal
    if "." not in value:
      raise AttributeError("Value does not contain a . so may be a valid integer")

    float(value).is_integer()
    counter[index][DECIMAL] += 1
  except (AttributeError, ValueError) as err:
    for x in list_datatypes[1:]:
      usevalue = ""
      n_type = ""
      d_type = ""
      try:
        if x in [INTEGER, LONG]:
          if x == INTEGER:
            usetype = int
            try:
              usevalue = int(value)
            except:
              pass
            n_type = INTEGER
            # This allows the LONG iteration step to be skipped
            if usevalue >= MAX_INT:
              usetype = long
              try:
                usevalue = long(value)
              except:
                pass
              n_type = LONG

          if usevalue != "" and isinstance(usevalue, usetype):
            counter[index][n_type] += 1
        elif x == DATETIME:
          # Work on the dates
          d_type = check_datetime(value)
          if d_type:
            if d_type not in counter[index]:
              counter[index][d_type] = 0
            counter[index][d_type] += 1

        ## At the last element, check to see if nothing was available then add as a string
        ## Add a check to exclude empty values from the calcs
        if x == DATETIME and (d_type == "" or n_type == "" and value != ""):
          counter[index][STRING] += 1
      except ValueError:
        pass



def check_valid(value, inindex):
  index = str(inindex)
  if index not in valid:
    valid[index] = {}

  atype = ""
  if value == "":
    #atype = EMPTY
    atype = NULL
  elif value == 0:
    #atype = ZERO
    atype = NULL
  else:
    #atype = VALUE
    atype = VALID
  if atype not in valid[index]:
    valid[index][atype] = 0
  valid[index][atype] += 1
    


def redo_valid():
  for k, v in counter.items():
    counter_invalid = 0
    for k2, v2 in v.iteritems():
      if k2 != STRING and v2 > 0:
        counter_invalid += 1
    if counter_invalid > 1:
      if k not in valid:
        valid[k] = {}
      if INVALID not in valid[k]:
        valid[k][INVALID] = ["datatype"]
      else: 
        valid[k].update( {INVALID:  valid[k][INVALID].append("datatype")} )
  
  for k, v in semantic.items():
    if len(v) > 1:
      if k not in valid:
        valid[k] = {}
      if INVALID not in valid[k]:
        valid[k][INVALID] = ["semantic"]
      else: 
        valid[k].update( {INVALID:  valid[k][INVALID] + ["semantic"]} )



def valid_append_data():
  keys = valid.keys()
  for k,v in valid.iteritems():
    counter_invalid = 0
    for k, v in counter[k].iteritems():
      if k != STRING and v > 0:
        counter_invalid += 1
    
    if counter_invalid > 1 or len(semantic[k]) > 1:
      v.update( {INVALID: True} )



def print_datatypes(desc, mapper):
  mapper = OrderedDict(sorted(mapper.items(), key=lambda k:int(k[0]), reverse=False))
  for x in mapper.keys():
    tempdesc = OrderedDict(sorted(mapper[x].items(), key=lambda k:k[1], reverse=True))
    
    keys = tempdesc.keys()
    maxfound = tempdesc[keys[0]]
    maxindex = keys[0]

    if keys[0] == STRING:
      for keyr in xrange(1, len(keys)):
        newkey = keys[keyr]
        if tempdesc[newkey] > 0 and tempdesc[newkey] >= (maxfound* (100-STRING_MARGIN)/100):
          maxfound = tempdesc[newkey]
          maxindex = keys[keyr]
    print " %s,%s:\t%s: %s ===> %s" % (x, desc, maxindex.upper(), tempdesc[maxindex], str(tempdesc)[11:])



def print_semantics(desc, mapper):
  mapper = OrderedDict(sorted(mapper.items(), key=lambda k:int(k[0]), reverse=False))
  for x in mapper.keys():
    tempdesc = OrderedDict(sorted(mapper[x].items(), key=lambda k:k[1], reverse=True))
   
    """
    ####print " %s[%s]:\t%s" % (desc, x, tempdesc)
    keys = tempdesc.keys()
    maxkey = ""
    maxvalue = ""
    if len(keys) >= 1:
      maxkey = keys[0]
      maxvalue = tempdesc[maxkey]
    print " %s[%s]:\t%s: %s" % (desc, x, maxkey.upper(), maxvalue)
    """

    print " %s,%s:\t%s" % (x, desc, str(tempdesc)[11:])
    


def print_valids(desc, mapper):
  mapper = OrderedDict(sorted(mapper.items(), key=lambda k:int(k[0]), reverse=False))
  for x in mapper.keys():
    stra = ""
    tempdesc = OrderedDict(sorted(mapper[x].items(), key=lambda k:k[1], reverse=True))
   
    if VALID in tempdesc:
      stra += "| VALID:" + str(tempdesc[VALID])
    if NULL in tempdesc:
      stra += "| NULL:" + str(tempdesc[NULL])
    if INVALID in tempdesc:
      stra += "| INVALID:" + ",".join(sorted(tempdesc[INVALID]))

    print " %s,%s:\t%s" % (x, desc, stra)



def check_semantic(value, inindex):
  index = str(inindex)
  if index not in semantic:
    semantic[index] = {}

  for k,v in semantic_formats.iteritems():
    regex = re.compile(k)
    match = regex.search(value)
    if match:
      if v not in semantic[index]:
        semantic[index][v] = 0
      semantic[index][v] += 1

  addstate = list(k for (k,v) in states.iteritems() if value.upper()==k.upper() or value.upper()==v.upper())
  if len(addstate) >= 1:
    if STATE not in semantic[index]:
      semantic[index][STATE] = 0
    semantic[index][STATE] += 1
    

  
def parsedata(txt):
  mapper = {}
  txt = txt.strip()
  index,invalue = txt.split("\t", 1)
  dtype,value = invalue.split(",", 1)

  if dtype[0] == "D":
    mapper = counter
  elif dtype[0] == "S":
    mapper = semantic
  elif dtype[0] == "V":
    mapper = valid

  if index not in mapper:
    mapper[index] = {}

  tempdict = ast.literal_eval(value)
  for elemk, elemv in tempdict.iteritems():
    if elemk not in mapper[index]:
      mapper[index].update( {elemk:0} )
    mapper[index][elemk] += elemv
 
  

##################################################################
#  START MAIN
##################################################################

for line in sys.stdin:
  parsedata(line)

print_datatypes("Datatypes", counter)
print_semantics("Semantics", semantic)
redo_valid()
print_valids("Validity", valid)
