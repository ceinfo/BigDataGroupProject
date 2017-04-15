# BigData-Project1
Our project analyzes NYC Crime Data from 2006 - 2015.  The source code available in this repository provides the following:
  - Map and Reduce scripts for Hadoop execution
  - Script to execute the map/reduce jobs, consolidate the output, and general environment cleanup
  - Sample output of the datatype, semantic, and validity analysis
  


## Getting Started

These instructions will get you setup and running with the project on your local machine for development and testing.  

1) Download the NYPD_Complaint_Data_Historic.csv dataset:  
  https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD
  
2) Download the code from this repository and place onto Hadoop cluster (ex:  dumbo)

3) Execute the Map/Reduce scripts by running any 3 possible commands below:
```
     a) ./test.sh NYPD_Complaint_Data_Historic.csv        (analyzes all columns)
     b) ./test.sh NYPD_Complaint_Data_Historic.csv #      (analyzes specific column # given; # range begins from 0)
     c) ./test.sh NYPD_Complaint_Data_Historic.csv #,#,#  (analyzes only columns specified)
```

4) View ./src/output


## Directory Structure
```
  test.sh   (script to execute the Hadoop jobs)
  src       (src and output directory)
  |_____ map.py     (map job)
  |_____ reduce.py  (reduce job)
  |_____ srctmp.out (intermediary hadoop output file, ignored)
  |_____ output     (consolidated output file)
       
```


## Sample Output
Output file:   ./src/output

There are 3 sections of the output file (Datatypes, Semantics, and Validity):

#### - Section 1: Datatypes
    Analysis on the possibile datatype matches.  The possible 
    datatypes validated are:  integer, long, decimal, datetime, date, 
    time, and string.  
    
    The format:
    index,Datatypes:   [LikelyDatatype: Count] ===> [CheckedDatatype, DatatypeCount, ...]
    
   
```
 0,Datatypes:   INTEGER: 5101231 ===> ([('integer', 5101231), ('string', 5101231), ('decimal', 0), ('long', 0), ('datetime', 0)])
 1,Datatypes:   DATE: 5100576 ===> ([('string', 5100576), ('date', 5100576), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 2,Datatypes:   TIME: 5100280 ===> ([('string', 5101183), ('time', 5100280), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 3,Datatypes:   DATE: 3709753 ===> ([('string', 3709753), ('date', 3709753), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 4,Datatypes:   TIME: 3712070 ===> ([('string', 3713446), ('time', 3712070), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 5,Datatypes:   DATE: 5101231 ===> ([('string', 5101231), ('date', 5101231), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 6,Datatypes:   INTEGER: 5101231 ===> ([('integer', 5101231), ('string', 5101231), ('decimal', 0), ('long', 0), ('datetime', 0)])
 7,Datatypes:   STRING: 5082391 ===> ([('string', 5082391), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 8,Datatypes:   INTEGER: 5096657 ===> ([('integer', 5096657), ('string', 5096657), ('decimal', 0), ('long', 0), ('datetime', 0)])
 9,Datatypes:   STRING: 5096657 ===> ([('string', 5096657), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 10,Datatypes:  STRING: 5101224 ===> ([('string', 5101224), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 11,Datatypes:  STRING: 5101231 ===> ([('string', 5101231), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 12,Datatypes:  STRING: 5101231 ===> ([('string', 5101231), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 13,Datatypes:  STRING: 5100768 ===> ([('string', 5100768), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 14,Datatypes:  INTEGER: 5100841 ===> ([('integer', 5100841), ('string', 5100841), ('decimal', 0), ('long', 0), ('datetime', 0)])
 15,Datatypes:  STRING: 3974103 ===> ([('string', 3974103), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 16,Datatypes:  STRING: 5067952 ===> ([('string', 5067952), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 17,Datatypes:  STRING: 7599 ===> ([('string', 7599), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 18,Datatypes:  STRING: 253205 ===> ([('string', 253205), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 19,Datatypes:  INTEGER: 4913085 ===> ([('integer', 4913085), ('string', 4913085), ('decimal', 0), ('long', 0), ('datetime', 0)])
 20,Datatypes:  INTEGER: 4913085 ===> ([('integer', 4913085), ('string', 4913085), ('decimal', 0), ('long', 0), ('datetime', 0)])
 21,Datatypes:  DECIMAL: 4913085 ===> ([('decimal', 4913085), ('integer', 0), ('long', 0), ('datetime', 0)])
 22,Datatypes:  DECIMAL: 4913085 ===> ([('decimal', 4913085), ('integer', 0), ('long', 0), ('datetime', 0)])
 23,Datatypes:  STRING: 4913085 ===> ([('string', 4913085), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
```

 
#### - Section 2: Semantics
```
  Analysis on the possible semantic matches.  The possible semantics 
  validated are:  phone, address, zipcode, state, latitude coords, longitude coords, 
  and email.
    
  The format:
  index,Semantics:   [MatchedSemantic,Count, ...]
```
 
```
 0,Semantics:   ()
 1,Semantics:   ()
 2,Semantics:   ()
 3,Semantics:   ()
 4,Semantics:   ()
 5,Semantics:   ()
 6,Semantics:   ()
 7,Semantics:   ()
 8,Semantics:   ()
 9,Semantics:   ()
 10,Semantics:  ()
 11,Semantics:  ()
 12,Semantics:  ()
 13,Semantics:  ()
 14,Semantics:  ()
 15,Semantics:  ()
 16,Semantics:  ()
 17,Semantics:  ()
 18,Semantics:  ([('address', 1989)])
 19,Semantics:  ()
 20,Semantics:  ()
 21,Semantics:  ([('latitude', 4913085), ('longitude', 4913085)])
 22,Semantics:  ([('latitude', 4913085), ('longitude', 4913085)])
 23,Semantics:  ()
```

#### - Section 3: Validity
```
  Analysis on the validity of the data.  The fields represented:
    - VALID:count - the number of valid records
    - NULL:count - the number of empty string records
    - INVALID:datatype,semantic - if there are inconsistencies and multiple datatypes/semantics are found
    
  Note:  If no data is available, then the field is omitted. 
  
  The format:
  index,Validity:   [VALID:Count| ...]
```
```
 0,Validity:    | VALID:5101231
 1,Validity:    | VALID:5100576| NULL:655
 2,Validity:    | VALID:5101183| NULL:48
 3,Validity:    | VALID:3709753| NULL:1391478
 4,Validity:    | VALID:3713446| NULL:1387785
 5,Validity:    | VALID:5101231
 6,Validity:    | VALID:5101231
 7,Validity:    | VALID:5082391| NULL:18840
 8,Validity:    | VALID:5096657| NULL:4574
 9,Validity:    | VALID:5096657| NULL:4574
 10,Validity:   | VALID:5101224| NULL:7
 11,Validity:   | VALID:5101231
 12,Validity:   | VALID:5101231
 13,Validity:   | VALID:5100768| NULL:463
 14,Validity:   | VALID:5100841| NULL:390
 15,Validity:   | VALID:3974103| NULL:1127128
 16,Validity:   | VALID:5067952| NULL:33279
 17,Validity:   | VALID:7599| NULL:5093632
 18,Validity:   | VALID:253205| NULL:4848026
 19,Validity:   | VALID:4913085| NULL:188146
 20,Validity:   | VALID:4913085| NULL:188146
 21,Validity:   | VALID:4913085| NULL:188146| INVALID:semantic
 22,Validity:   | VALID:4913085| NULL:188146| INVALID:semantic
 23,Validity:   | VALID:4913085| NULL:188146                    
```

Thanks!
