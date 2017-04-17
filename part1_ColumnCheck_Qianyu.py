import pyspark
from csv import reader

lines_parking = sc.textFile('NYPD_Complaint_Data_Historic.csv', 1) 
header = lines_parking.first()
lines_parking = lines_parking.filter(lambda x: x != header)
lines_parking = lines_parking.mapPartitions(lambda x: reader(x))

KY_CD = lines_parking.map(lambda x:x[6])
OFNS_DESC = lines_parking.map(lambda x:x[7])
PD_CD = lines_parking.map(lambda x:x[8])
PD_DESC = lines_parking.map(lambda x:x[9])
CRM_ATPT_CPTD_CD = lines_parking.map(lambda x:x[10])
LAW_CAT_CD = lines_parking.map(lambda x:x[11])
JURIS_DESC = lines_parking.map(lambda x:x[12])

KY_CD.map(lambda x:(x,"NUMERIC","offense_classification_code","VALID" if len(x)==3 else "INVALID"))
OFNS_DESC.map(lambda x:(x,"TEXT","Description_of_offense","VALID" if len(x)>0 else "NULL"))
PD_CD.map(lambda x:(x,"NUMERIC","internal_classification_code","VALID" if len(x)==3 else "NULL"))
PD_DESC.map(lambda x:(x,"TEXT","Description_internal_classification","VALID" if len(x)>0 else "NULL"))
CRM_ATPT_CPTD_CD.map(lambda x:(x,"TEXT","Crime_Status","VALID" if x in ["COMPLETED","ATTEMPTED"] else "NULL"))
LAW_CAT_CD.map(lambda x:(x,"TEXT","Level_of_offense","VALID" if x in ["FELONY","MISDEMEANOR","VIOLATION"] else "NULL"))
JURIS_DESC.map(lambda x:(x,"TEXT","Jurisdiction_for_incident","VALID" if len(x)>0 else "NULL"))
