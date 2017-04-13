from csv import reader
from validators import *
import pyspark
import os
import shutil

sc = pyspark.SparkContext.getOrCreate()

# read data into RDD
data = sc.textFile('./NYPD_Complaint_Data_Historic.csv', 1) \
          .mapPartitions(lambda x: reader(x))

# get column names and remove header
header = data.first()
data = data.filter(lambda x: x != header)

cols = [None]*24

cols[CMPLNT_NUM] = data.map(lambda x: (x[CMPLNT_NUM], 'NUMERIC', 'Unique ID', validate_ID(x[CMPLNT_NUM])))
cols[CMPLNT_FR_DT] = data.map(lambda x: (x[CMPLNT_FR_DT], 'DATE', 'Starting date of occurrence' if len(x[CMPLNT_TO_DT])>0 else 'Exact date of occurence', validate_date(x[CMPLNT_FR_DT])))
cols[CMPLNT_TO_DT] = data.map(lambda x: (x[CMPLNT_TO_DT], 'DATE', 'Ending date of occurrence' if len(x[CMPLNT_TO_DT])>0 else 'Null', validate_date(x[CMPLNT_TO_DT])))
cols[CMPLNT_FR_TM] = data.map(lambda x: (x[CMPLNT_FR_TM], 'TIME', 'Starting time of occurence' if len(x[CMPLNT_TO_TM])>0 else 'Exact time of occurence', validate_time(x[CMPLNT_FR_TM])))
cols[CMPLNT_TO_TM] = data.map(lambda x: (x[CMPLNT_TO_TM], 'TIME', 'Ending time of occurence' if len(x[CMPLNT_TO_TM])>0 else 'Null', validate_date(x[CMPLNT_TO_TM])))
cols[RPT_DT] = data.map(lambda x: (x[RPT_DT], 'DATE', 'Reported date' if len(x[RPT_DT])>0 else 'Null', validate_date(x[RPT_DT], begin=2006, end=2016)))
cols[KY_CD] = data.map(lambda x:(x[KY_CD],"NUMERIC","offense_classification_code","VALID" if len(x[KY_CD])==3 else "INVALID"))
cols[OFNS_DESC] = data.map(lambda x:(x[OFNS_DESC],"TEXT","Description_of_offense","VALID" if len(x[OFNS_DESC])>0 else "NULL"))
cols[PD_CD] = data.map(lambda x:(x[PD_CD],"NUMERIC","internal_classification_code","VALID" if len(x[PD_CD])==3 else "NULL"))
cols[PD_DESC] = data.map(lambda x:(x[PD_DESC],"TEXT","Description_internal_classification","VALID" if len(x[PD_DESC])>0 else "NULL"))
cols[CRM_ATPT_CPTD_CD] = data.map(lambda x:(x[CRM_ATPT_CPTD_CD],"TEXT","Crime_Status","VALID" if x[CRM_ATPT_CPTD_CD] in ["COMPLETED","ATTEMPTED"] else "NULL"))
cols[LAW_CAT_CD] = data.map(lambda x:(x[LAW_CAT_CD],"TEXT","Level_of_offense","VALID" if x[LAW_CAT_CD] in ["FELONY","MISDEMEANOR","VIOLATION"] else "NULL"))
cols[JURIS_DESC] = data.map(lambda x:(x[JURIS_DESC],"TEXT","Jurisdiction_for_incident","VALID" if len(x[JURIS_DESC])>0 else "NULL"))
cols[PARKS_NM] = data.map(lambda x:(x[PARKS_NM],'TEXT','Name of NYC park, playground or greenspace',text_valuecheck(x[PARKS_NM])))
cols[PREM_TYP_DESC] = data.map(lambda x:(x[PREM_TYP_DESC],'TEXT','Specific description of premises',text_valuecheck(x[PREM_TYP_DESC])))
cols[LOC_OF_OCCUR_DESC] = data.map(lambda x:(x[LOC_OF_OCCUR_DESC],'TEXT','Specific location of occurrence in or around the premises',text_valuecheck(x[LOC_OF_OCCUR_DESC])))
cols[HADEVELOPT] = data.map(lambda x:(x[HADEVELOPT],'TEXT','Name of NYCHA housing development',text_valuecheck(x[HADEVELOPT])))
cols[BORO_NM] = data.map(lambda x: (x[BORO_NM], 'TEXT', 'Borough name', borough_valuecheck(x[BORO_NM],x[ADDR_PCT_CD])))
cols[ADDR_PCT_CD] = data.map(lambda x: (x[ADDR_PCT_CD], 'Integer', 'Precinct ID', precinct_valuecheck(x[BORO_NM],x[ADDR_PCT_CD])))
cols[X_COORD_CD] = data.map(lambda x: (x[X_COORD_CD],'Integer', 'X-coordinate for New York State Plane Coordinate System', location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'X')))
cols[Y_COORD_CD] = data.map(lambda x: (x[Y_COORD_CD],'Integer', 'X-coordinate for New York State Plane Coordinate System', location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'Y')))
cols[Latitude] = data.map(lambda x:(x[Latitude],'Float','Latitude coordinate for Global Coordinate System', location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'LA')))
cols[Longitude] = data.map(lambda x:(x[Longitude],'Float','Longitude coordinate for Global Coordinate System', location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'LO')))
cols[Lat_Lon] = data.map(lambda x:(x[Lat_Lon],'Location','Location in Global Coordinate System', location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'GPS')))

for i in range(len(cols)):
	outdir = 'col_%d_%s' % (i, header[i])
	# delete if output directory exists
	if outdir in os.listdir():
		shutil.rmtree(outdir)
	# save as text file using format name
	if cols[i]!=None:
		cols[i].saveAsTextFile(outdir)