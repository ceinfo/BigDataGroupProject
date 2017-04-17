## Run ETL scripts

### Run data_check

This script aims to check certain anomalies or relationship and output the results for manual review. To run the script, you need to upload the data to hadoop file system. E.g., if the uploaded file name is 'NYPD.csv', you can run the script by (Note: you need to remove existing output files, if any):

```
spark-submit data_check.py NYPD.csv
```

Four output files are generated, including:
1. crime_dataCheck.out
2. cntByDay.out
3. KYPD_tab.out
4. KYDesp_tab.out

Files can be downloaded to your home directory by the hfs getmerge command.
