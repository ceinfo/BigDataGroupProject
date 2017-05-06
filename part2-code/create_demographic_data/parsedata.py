#!/usr/bin/python

###########################################################
#
# parsedata.py - parses data from nyc open data for 
#  ACS/community data.  Modifications allow rewrite of
#  modified csv into an  updated csv which groups 
#  datasets by neighborhoods in the NY community. This 
#  makes it easier for the team to process the data. 
#
###########################################################

import csv
import sys 

row_desc = []
neighborhood = []
data = {}


############################################################
#  Beginning MAIN
############################################################

counter = 0
reference_notes_found = False
with open(sys.argv[1]) as f:
    ##reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        # Reference notes found at the end, stop processing data when found
        if "REFERENCE NOTES:" in row[0]:
          reference_notes_found = True

        # Processing neighborhood data
        if "BK72" in row[1]:  
          # each neighborhood that is not empty, append as field
          prev_elem = ""
          for elem in row:
            if elem != "" and prev_elem != elem.split (" ", 1)[0]:
              neighborhood.append(elem)
              prev_elem = elem.split (" ", 1)[0]

        # Processing data elems for each neighborhood
        if row[0] != "" and counter >= 7 and not reference_notes_found: 
          found_header = "***" if row[2] == row[3] == "" else ""
          row_desc.append(found_header + row[0])

          # parse each row's element and assign to the right neighborhood
          # neig0=1, neigh1=5,  neigh2=9
          # 4*2 + 1
          # 4*1 + 1
          # 4*0 + 1 
          for natdata in xrange(0,len(neighborhood)): 
            code = neighborhood[natdata]
            if code not in data:
              data[code] = []
            if row[1] != "" and row[2] != "":
              data[code].append( row[natdata*4 + 1] )
            elif row[2] == row[3] == "":
              data[code].append( "***" + row[0] )
            else:
              data[code].append("")
    

        counter += 1
f.close()


delim = "|"
newrow = ["DESC", "NTACODE", "NTANAME"] + row_desc
newrow_range = range(0, len(newrow))
newrow =  map(lambda x,y:  x + " (" + str(y) + ")", newrow, newrow_range)
print delim.join(newrow).upper()

for x in neighborhood:
  code, name = x.split(" ", 1)
  print "DATA%s%s%s%s%s%s" % (delim, code, delim, name, delim, delim.join(data[x]))

