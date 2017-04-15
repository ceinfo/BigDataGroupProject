#!/usr/bin/python

"""
  Author:  Catherine Eng
  This code analyzes the NYC Crime Data file.  Will parse through and pick out anomalies, 
  some interesting pieces of the data in our research.
"""

import csv
import os
import re
import sys
import time
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

  if value in found_regex and len(counter[index]) > 0:
    for k in found_regex[value].keys():
        if k not in counter[index]:
          counter[index][k] = found_regex[value][k]
        else:
          counter[index][k] = sum( [ counter[index][k], found_regex[value][k] ])
    return
  
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
    temp[DECIMAL] = 1
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
            temp[n_type] = 1
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
            found_datetime[value] = d_type

        ## At last element, check to see if nothing was available then add as a string
        ## Add a check to exclude empty values from the calcs
        if x == DATETIME and (d_type == "" or n_type == "" and value != ""):
          if STRING not in counter[index]:
            counter[index][STRING] = 0
          counter[index][STRING] += 1
          temp[STRING] = 1
      except ValueError:
        pass

  # Add as our last step
  found_regex[value] = temp



def check_valid(value, inindex, entry):
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
  elif inindex in [1,3,5]:
    rules_found = []
    fromd = ""
    try:
      fromd = time.strptime(entry[1], "%m/%d/%Y")
      tod = time.strptime(entry[3], "%m/%d/%Y")
    except:
      tod = ""
    try:
      reportd = time.strptime(entry[5], "%m/%d/%Y")
    except:
      reportd = ""
    # Invalid rules are tested on 3 conditions:
    # 1 - when date < 1957
    # 2 - when from date > to date
    # 3 - when from date > report date

    # Calculate rule 1.
    if value[5] != "/" or value[-4:] < '1957':
      rules_found = ["1"]
    # Calculate rule 2.
    if inindex == 1 or inindex == 3:
      if fromd > tod:
        rules_found.append("2")
    # Calculate rule 3.
    if inindex == 1 or inindex == 5:
      if fromd > reportd:
        rules_found.append("3")
    # Append invalid + failed rule.
    if len(rules_found) > 0:
      atype = INVALID + "(" + ",".join(rules_found) + ")"

  # If no errors, then valid.
  if atype == "":
    atype = VALID

  if atype not in valid[index]:
    valid[index][atype] = 0
  valid[index][atype] += 1
    


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

    print " %s\t%s,%s" % (x, desc, mapper[x])



def print_valids(desc, mapper):
  mapper = OrderedDict(sorted(mapper.items(), key=lambda k:int(k[0]), reverse=False))
  for x in mapper.keys():
    stra = ""
    tempdesc = OrderedDict(sorted(mapper[x].items(), key=lambda k:k[1], reverse=True))
   
    if NULL in tempdesc:
      stra += "| NULL:" + str(tempdesc[NULL])
    for key, value in tempdesc.items():
       if INVALID in key:
         stra += " |%s: %s" % (key, value)
    if VALID in tempdesc:
      stra += "| VALID:" + str(tempdesc[VALID])
  
    print (" %s\t%s,%s") % (x, desc, mapper[x])



def check_semantic(value, inindex):
  index = str(inindex)
  if index not in semantic:
    semantic[index] = {}

  for k,v in semantic_formats.iteritems():
    cvalue = value
    if v == ADDRESS:
      cvalue = value.lower()
    match = regex_semantics[k].search(cvalue)
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
    
  for len_itr in search_columns:
    value = entry[len_itr]
    check_datatype(value, len_itr)
    check_semantic(value, len_itr)
    check_valid(value, len_itr, entry)

  ##valid_append_data()



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

print_datatypes("Datatypes", counter)
print_semantics("Semantics", semantic)
print_valids("Validity", valid)

