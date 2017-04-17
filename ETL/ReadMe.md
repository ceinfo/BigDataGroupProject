## Run ETL scripts
Note: for all of the following scripts, you need to remove existing output files, if any

### Run data_check

This script aims to check certain anomalies or relationship and output the results for manual review. To run the script, you need to upload the data to hadoop file system. E.g., we have an uploaded file in '/tmp/newgrp/NYPD_Complaint_Data_Historic.csv', you can run the script by:

```
spark-submit data_check.py /tmp/newgrp/NYPD_Complaint_Data_Historic.csv
```

Four output files are generated, including:
1. crime_dataCheck.out
2. cntByDay.out
3. KYPD_tab.out
4. KYDesp_tab.out

Files can be downloaded to your home directory by the hfs getmerge command.

### Run data_clean

This script cleans the data based on identified invalid values. To run the script, use:

```
spark-submit data_clean.py /tmp/newgrp/NYPD_Complaint_Data_Historic.csv
```

One output file with name 'NYPD_clean.out' is generated, files can be downloaded to home directory by the hfs getmerge command

### Run data_check_dup

This script goes through the cleaned dataset to check for duplications in all columns except for the ID column. The input is the cleaned data, which we uploaded to the hfs as /tmp/newgrp/NYPD_clean.txt. To run the script, use:

```
spark-submit data_check_dup.py /tmp/newgrp/NYPD_clean.txt
```

One output file with name 'Dups.out' is generated. 
