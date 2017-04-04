# BigData-Project1
Our project analyzes NYC Crime Data from 2006 - 2015.  The source code available in this repository provides the following:
  - Map and Reduce scripts for Hadoop execution
  - Script to execute the map/reduce jobs, consolidate the output, and general environment cleanup
  - Sample output of the datatype, semantic, and validity analysis
  


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 
1) Download "NYPD_Complaint_Data_Historic.csv" DataSet:  
  https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD
  
2) Download the code from this repository and place onto Hadoop cluster (ex:  dumbo)

3) Run ./test.sh NYPD_Complaint_Data_Historic.csv


## Directory Structure
```
  test.sh   (script to execute the Hadoop jobs)
  src       (src and output directory)
  |_____ map.py     (map job)
  |_____ reduce.py  (reduce job)
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
    index,Datatypes:   [LikelyDatatype: Counter] ===> [CheckedDatatype: DatatypeCount, ...]
    
   
```
 0,Datatypes:   INTEGER: 3306502 ===> ([('integer', 3306502), ('string', 3306502), ('decimal', 0), ('long', 0), ('datetime', 0)])
 1,Datatypes:   DATE: 3306204 ===> ([('string', 3306204), ('date', 3306204), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 2,Datatypes:   TIME: 3306434 ===> ([('string', 3306491), ('time', 3306434), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 3,Datatypes:   DATE: 2504763 ===> ([('string', 2504763), ('date', 2504763), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 4,Datatypes:   TIME: 2505950 ===> ([('string', 2506035), ('time', 2505950), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 5,Datatypes:   DATE: 3306502 ===> ([('string', 3306502), ('date', 3306502), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 6,Datatypes:   INTEGER: 3306502 ===> ([('integer', 3306502), ('string', 3306502), ('decimal', 0), ('long', 0), ('datetime', 0)])
 7,Datatypes:   STRING: 3295888 ===> ([('string', 3295888), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 8,Datatypes:   INTEGER: 3303692 ===> ([('integer', 3303692), ('string', 3303692), ('decimal', 0), ('long', 0), ('datetime', 0)])
 9,Datatypes:   STRING: 3303692 ===> ([('string', 3303692), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 10,Datatypes:  STRING: 3306498 ===> ([('string', 3306498), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 11,Datatypes:  STRING: 3306502 ===> ([('string', 3306502), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 12,Datatypes:  STRING: 3306502 ===> ([('string', 3306502), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 13,Datatypes:  STRING: 3306428 ===> ([('string', 3306428), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 14,Datatypes:  INTEGER: 3306501 ===> ([('integer', 3306501), ('string', 3306501), ('decimal', 0), ('long', 0), ('datetime', 0)])
 15,Datatypes:  STRING: 2581201 ===> ([('string', 2581201), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 16,Datatypes:  STRING: 3291891 ===> ([('string', 3291891), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 17,Datatypes:  STRING: 7599 ===> ([('string', 7599), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 18,Datatypes:  STRING: 166259 ===> ([('string', 166259), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])
 19,Datatypes:  INTEGER: 3189168 ===> ([('integer', 3189168), ('string', 3189168), ('decimal', 0), ('long', 0), ('datetime', 0)])
 20,Datatypes:  INTEGER: 3189168 ===> ([('integer', 3189168), ('string', 3189168), ('decimal', 0), ('long', 0), ('datetime', 0)])
 21,Datatypes:  DECIMAL: 3189168 ===> ([('decimal', 3189168), ('integer', 0), ('string', 0), ('long', 0), ('datetime', 0)])
 22,Datatypes:  DECIMAL: 3189168 ===> ([('decimal', 3189168), ('integer', 0), ('string', 0), ('long', 0), ('datetime', 0)])
 23,Datatypes:  STRING: 3189168 ===> ([('string', 3189168), ('integer', 0), ('decimal', 0), ('long', 0), ('datetime', 0)])

```

 
#### - Section 2: Semantics
```
  Analysis on the possible semantic matches.  The possible semantics 
  validated are:  phone, addres, zipcode, state, latitude coords, longitude coords, 
  and email.
    
  The format:
  index,Semantics:   [MatchedSemantic: Counter, ...]
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
 18,Semantics:  ([('address', 1157)])
 19,Semantics:  ()
 20,Semantics:  ()
 21,Semantics:  ([('latitude', 3189168), ('longitude', 3189168)])
 22,Semantics:  ([('latitude', 3189168), ('longitude', 3189168)])
 23,Semantics:  ()
```

#### - Section 3: Validity
```
  Analysis on the validity of the data.  The fields represented:
    - VALID:count - the number of valid records
    - NULL:count - the number of empty string records
    - INVALID:datatype, semantic - if there are inconsistencies and multiple datatypes/semantics found
    
  Note:  If no data, then the field is omitted. 
  
  The format:
  index,Validity:   [VALID: Counter, ...]
```
```
 0,Validity:    | VALID:3306502
 1,Validity:    | VALID:3306204| NULL:298
 2,Validity:    | VALID:3306491| NULL:11
 3,Validity:    | VALID:2504763| NULL:801739
 4,Validity:    | VALID:2506035| NULL:800467
 5,Validity:    | VALID:3306502
 6,Validity:    | VALID:3306502
 7,Validity:    | VALID:3295888| NULL:10614
 8,Validity:    | VALID:3303692| NULL:2810
 9,Validity:    | VALID:3303692| NULL:2810
 10,Validity:   | VALID:3306498| NULL:4
 11,Validity:   | VALID:3306502
 12,Validity:   | VALID:3306502
 13,Validity:   | VALID:3306428| NULL:74
 14,Validity:   | VALID:3306501| NULL:1
 15,Validity:   | VALID:2581201| NULL:725301
 16,Validity:   | VALID:3291891| NULL:14611
 17,Validity:   | VALID:7599| NULL:3298902
 18,Validity:   | VALID:166259| NULL:3140242
 19,Validity:   | VALID:3189168| NULL:117333
 20,Validity:   | VALID:3189168| NULL:117333
 21,Validity:   | VALID:3189168| NULL:117333| INVALID:semantic
 22,Validity:   | VALID:3189168| NULL:117333| INVALID:semantic
 23,Validity:   | VALID:3189168| NULL:117333                                                             
```


