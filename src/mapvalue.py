#!/usr/bin/python

"""
  Author:  Catherine Eng
  This code analyzes the NYC Crime Data file.  Will parse through and pick out anomalies, 
  some interesting pieces of the data in our research.
"""

import csv
import operator
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime

counter = {}
semantic = {}
valid = {}
column_iterator = []
found_regex = {}
found_datetime = {}
found_entry_list = []
found_entry_len = 0

MAX_INT = 2147483647
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
LATITUDE = "latitude"
LONGITUDE = "longitude"

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
    "^[(\d{3})]*[-]*\d{3}-\d{4}$":  PHONE,
    "^\d{5}-\d{4}$":  ZIPCODE,
    "^\d{5}$":  ZIPCODE,
    "^\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)":  ADDRESS,
    "(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)":  EMAIL, 
    "^[-+]?([1-8]?\d(\.\d+)|90(\.0+)+)$":  LATITUDE,
    "^[-+]?(180(\.0+)|((1[0-7]\d)|([1-9]?\d))(\.\d+))$":  LONGITUDE,
}

regex_semantics = {}
for k,v in semantic_formats.iteritems():
  regex = re.compile(k)
  regex_semantics[k] = regex

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

expected_datatypes = [
  INTEGER,
  DATE,
  TIME,
  DATE,
  TIME,
  DATE,
  INTEGER,
  STRING,
  INTEGER,
  STRING,
  STRING,
  STRING,
  STRING,
  STRING,
  INTEGER,
  STRING,
  STRING,
  STRING,
  STRING,
  INTEGER,
  INTEGER,
  DECIMAL,
  DECIMAL,
  STRING,
]



def check_datetime(value):
  for x in date_formats:
    try:
      datetime.strptime(value, x)
      return date_formats[x] 
    except ValueError:
      continue



def check_datatype(value, inindex):
  temp = {}
  index = str(inindex)

  if index not in counter:
    counter[index] = {}

  ##if value in found_regex and len(counter[index]) > 0:
  if value in found_regex:
    if len(found_regex[value]) == 0:
      found_regex[value][STRING] = 1

    for k in found_regex[value].keys():
        if k not in counter[index]:
          counter[index][k] = found_regex[value][k]
        else:
          counter[index][k] = sum( [ counter[index][k], found_regex[value][k] ])
    return  str(max(counter[index].iteritems(), key=operator.itemgetter(1))[0])
  

  for elem in list_datatypes:
    x = str(elem)
    if x not in counter[index]:
      counter[index][x] = 0


  found_dtype = STRING
  try:
    # Is formatted as a decimal
    if "." not in value:
      raise AttributeError("Value does not contain a . so may be a valid integer")

    float(value).is_integer()
    counter[index][DECIMAL] += 1
    temp[DECIMAL] = 1
    found_dtype = DECIMAL
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
              n_type = INTEGER
            except:
              pass
            # This allows the LONG iteration step to be skipped
            if usevalue >= MAX_INT:
              usetype = long
              try:
                usevalue = long(value)
                n_type = LONG
              except:
                pass

          if n_type != STRING and isinstance(usevalue, usetype):
            counter[index][n_type] += 1
            temp[n_type] = 1
            found_dtype = n_type if found_dtype == STRING  else found_dtype
        elif x == DATETIME:
          # Work on the dates
          if value in found_datetime:
            d_type = found_datetime[value]
          else:
            d_type = check_datetime(value)
          if d_type:
            if d_type not in counter[index]:
              counter[index][d_type] = 0
            counter[index][d_type] += 1
            temp[d_type] = 1
            found_dtype = d_type if found_dtype == STRING else found_dtype
            found_datetime[value] = d_type
        ## At last element, check to see if nothing was available then add as a string
        ## Add a check to exclude empty values from the calcs
        else:
          if STRING not in counter[index]:
            counter[index][STRING] = 0
          counter[index][STRING] += 1
          temp[STRING] = 1
      except ValueError:
        pass

  # Add as our last step
  found_regex[value] = temp
  return found_dtype



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
  return atype
    


def valid_append_data():
  keys = valid.keys()
  for k,v in valid.iteritems():
    counter_invalid = 0
    for k2, v2 in counter[k].iteritems():
      if k2 != STRING and v2 > 0:
        counter_invalid += 1
    if counter_invalid > 1:
      v.update( {INVALID: True} )



def print_datatypes(desc, mapper):
  mapper = OrderedDict(sorted(mapper.items(), key=lambda k:int(k[0]), reverse=False))
  for x in mapper.keys():
    tempdesc = OrderedDict(sorted(mapper[x].items(), key=lambda k:k[1], reverse=True))
    print " %s\t%s,%s" % (x, desc, mapper[x])
   


def print_semantics(desc, mapper):
  mapper = OrderedDict(sorted(mapper.items(), key=lambda k:int(k[0]), reverse=False))
  for x in mapper.keys():
    tempdesc = OrderedDict(sorted(mapper[x].items(), key=lambda k:k[1], reverse=True))
    print " %s\t%s,%s" % (x, desc, mapper[x])



def print_valids(desc, mapper):
  mapper = OrderedDict(sorted(mapper.items(), key=lambda k:int(k[0]), reverse=False))
  for x in mapper.keys():
    stra = ""
    tempdesc = OrderedDict(sorted(mapper[x].items(), key=lambda k:k[1], reverse=True))
   
    if NULL in tempdesc:
      stra += "| NULL:" + str(tempdesc[NULL])
    if INVALID in tempdesc:
      stra += "| INVALID:" + str(tempdesc[INVALID])
    if VALID in tempdesc:
      stra += "| VALID:" + str(tempdesc[VALID])
  
    print (" %s\t%s,%s") % (x, desc, mapper[x])



def check_semantic(value, inindex):
  index = str(inindex)
  found_newsemantic = []
  if index not in semantic:
    semantic[index] = {}

  for k,v in semantic_formats.iteritems():
    cvalue = value
    if v == ADDRESS:
      cvalue = value.lower()
    match = regex_semantics[k].search(cvalue)
    if match:
      found_newsemantic.append(v)
      if v not in semantic[index]:
        semantic[index][v] = 0
      semantic[index][v] += 1

  addstate = list(k for (k,v) in states.iteritems() if value.upper()==k.upper() or value.upper()==v.upper())
  if len(addstate) >= 1:
    found_newsemantic.append(STATE)
    if STATE not in semantic[index]:
      semantic[index][STATE] = 0
    semantic[index][STATE] += 1

  ret_semantic = ",".join(found_newsemantic)
  return (ret_semantic)
    


def check_col0(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str 



def check_col1(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str 
  


def check_col2(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col3(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col4(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col5(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col6(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col7(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col8(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col9(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str


def check_col10(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str


def check_col11(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col12(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col13(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col14(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col15(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str


def check_col16(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col17(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col18(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col19(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col20(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col21(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col22(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def check_col23(value, len_itr):
  str = ""
  str += ("%s|%s") % (value, expected_datatypes[len_itr])
  str += ("|pickup %s") % (check_datatype(value, len_itr))
  str += ("|%s") % (check_semantic(value, len_itr))
  str += ("|%s") % (check_valid(value, len_itr))
  str += "|"
  return str



def parsedata(txt):
  global found_entry_len
  global found_entry_value
  txt = txt.strip()
  for x in csv.reader( [txt] ):
    entry = x

  if not column_iterator:
    if found_entry_len == 0 or found_entry_len != len(entry):
      found_entry_len = len(entry)
      found_entry_value = xrange(0, found_entry_len)
    search_columns = found_entry_value
  else:
    search_columns = column_iterator
    
  str = ""
  for len_itr in search_columns:
    value = entry[len_itr]

    if len_itr == 0:
      str += check_col0(value, len_itr)
    if len_itr == 1:
      str += check_col1(value, len_itr)
    if len_itr == 2:
      str += check_col2(value, len_itr)
    if len_itr == 3:
      str += check_col3(value, len_itr)
    if len_itr == 4:
      str += check_col4(value, len_itr)
    if len_itr == 5:
      str += check_col5(value, len_itr)
    if len_itr == 6:
      str += check_col6(value, len_itr)
    if len_itr == 7:
      str += check_col7(value, len_itr)
    if len_itr == 8:
      str += check_col8(value, len_itr)
    if len_itr == 9:
      str += check_col9(value, len_itr)
    if len_itr == 10:
      str += check_col10(value, len_itr)
    if len_itr == 11:
      str += check_col11(value, len_itr)
    if len_itr == 12:
      str += check_col12(value, len_itr)
    if len_itr == 13:
      str += check_col13(value, len_itr)
    if len_itr == 14:
      str += check_col14(value, len_itr)
    if len_itr == 15:
      str += check_col15(value, len_itr)
    if len_itr == 16:
      str += check_col16(value, len_itr)
    if len_itr == 17:
      str += check_col17(value, len_itr)
    if len_itr == 18:
      str += check_col18(value, len_itr)
    if len_itr == 19:
      str += check_col19(value, len_itr)
    if len_itr == 20:
      str += check_col20(value, len_itr)
    if len_itr == 21:
      str += check_col21(value, len_itr)
    if len_itr == 22:
      str += check_col22(value, len_itr)
    if len_itr == 23:
      str += check_col23(value, len_itr)
  print str[:-1]



##################################################################
#  START MAIN
##################################################################

# To view by specific columns, set your column_iterator=#,#,#
try:
  if len(sys.argv) > 1:
    env_columns = sys.argv[1]
  column_iterator = map(int,env_columns.split(","))
except:
  column_iterator = ""


for line in sys.stdin:
  if line[0] == "C":
    continue
  parsedata(line)

