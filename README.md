# big-data-project

## Part I Script
The part one script can be run by using the following command
```
spark-submit validation_script.py --data PATH-TO-FILE
```
where ```PATH-TO-FILE``` is the path to the input data csv file.
The outputs will be stored as partitioned text files in directories, with each directory representing one column named in the following convention:
col_```col_id```_```col_name```

The dataset is available for download at https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
