import pyspark
from pyspark.mllib.stat import Statistics
from csv import reader
import pandas as pd
import numpy as np
import re
from pyproj import Proj,transform

# read data into RDD
data = sc.textFile('./NYPD_Complaint_Data_Historic.csv', 1) \
          .mapPartitions(lambda x: reader(x))
header = data.first()
data = data.filter(lambda x: x != header)
# Column name setting
CMPLNT_NUM=0
CMPLNT_FR_DT=1
CMPLNT_FR_TM=2
CMPLNT_TO_DT=3
CMPLNT_TO_TM=4
RPT_DT=5
KY_CD=6
OFNS_DESC=7
PD_CD=8
PD_DESC=9
CRM_ATPT_CPTD_CD=10
LAW_CAT_CD=11
JURIS_DESC=12
LOC_OF_OCCUR_DESC=15
PREM_TYP_DESC=16
PARKS_NM=17
HADEVELOPT=18
BORO_NM=13
ADDR_PCT_CD=14
X_COORD_CD=19
Y_COORD_CD=20
Latitude=21
Longitude=22
Lat_Lon = 23

# Borough and Precinct Match
Borough = {'QUEENS':range(100,116), 'MANHATTAN':[1,5,6,7,9,10,13,14,17,18,19,20,22,23,24,25,26,28,30,32,33,34], 'BROOKLYN':[60,61,62,63,66,67,68,69,70,71,72,73,75,76,77,78,79,81,83,84,88,90,94], 'BRONX':list(range(40,51) ) +[52], 'STATEN ISLAND':list(range(120,124)), '':[-1]}
for key in Borough.keys():
    Borough[key] = [str(x) for x in Borough[key]] 

precictdict = {}
for key, value in Borough.items():
    for string in value:
        precictdict.setdefault(string, []).append(key)
precictdict[''] = ''

def borough_valuecheck(borough,precinct):
	'''
	check whether value in BORO_NM is a valid Borough namd and consistent with the precinct code
	'''
    if borough=='':
        return 'NULL'
    else:
        if (borough.upper() in Borough.keys()) and precinct in Borough[borough]:
            return 'Valid'
        else:
            return 'Invalid'

def precinct_valuecheck(borough,precinct):
	'''
	heck whether value in ADDR_PCT_CD is a valid Precinct ID and consistent with the Borough
	'''
    if precinct=='':
        return 'NULL'
    else:
        if (precinct in precictdict.keys()) and precinct in Borough[borough]:
            return 'Valid'
        else:
            return 'Invalid'
# Borough        
Borough_check  =  data.map(lambda x: (x[BORO_NM], 'TEXT', 'Borough name', borough_valuecheck(x[BORO_NM],x[ADDR_PCT_CD])))
# Precinct
Precinct_check  =  data.map(lambda x: (x[ADDR_PCT_CD], 'Integer', 'Precinct ID', precinct_valuecheck(x[BORO_NM],x[ADDR_PCT_CD])))


# geographic imformation match checking
def check_Projection(X_cordi,Y_cordi,latitud, longitude):
	'''
	Location information consistency under New York State Plane Coordinate System and Global Coordinate System
	Args:
	   X_cordi: X_COORD_CD varible 
	   Y_cordi: Y_COORD_CD varible 
	   latitud: Latitude varible 
	   longitude:  Longitude varible 
	Returns:
	   Boolean indicator for consistency
	'''
    if X_cordi!='' and Y_cordi!='' and latitud!='' and longitude!='':
        p = Proj(init="EPSG:2263", preserve_units=True)
        lon,la = p(X_cordi,Y_cordi,inverse=True) 
        if abs(lon-float(longitude))<10e-4 and abs(la-float(latitud))<10e-4:
            return True
        else:
            return False
    else:
        return False

def check_Lat_Lon(latitud, longitude, gps_com):
	'''
    Global Coordinate System Location information consistency between latitude, longitude and Lat_Lon
	'''
    if latitud!='' and longitude!='' and gps_com!='' and '(%s, %s)' % (latitud, longitude) == gps_com:
        return True
    else:
        return False

def location_valuecheck(X_cordi,Y_cordi,latitud, longitude, gps_com, col):
	'''
	Location related variable checker. 
	Args:
	  X_cordi,Y_cordi,latitud, longitude, gps_com
	  col: variable indicator. Possible values: 'X':X_cordi ,'Y': Y_cordi, 'LA':latitud, 'LO':longitude, 'GPS':gps_com
	'''
    dict_missing = {'X':X_cordi ,'Y': Y_cordi, 'LA':latitud, 'LO':longitude, 'GPS':gps_com}
    if dict_missing[col]=='':
        return 'NULL'
    else:  
        if (col in ['X','Y'] and check_Projection(X_cordi,Y_cordi,latitud, longitude)) or (col in ['LA','LO'] and check_Projection(X_cordi,Y_cordi,latitud, longitude) and check_Lat_Lon(latitud, longitude,gps_com)) or (col == 'GPS' and check_Lat_Lon(latitud, longitude,gps_com)):
            return 'Valid'
        else:
            return 'Invalid'


# X_COORD_CD 
X_COORD_CD_check = data.map(lambda x: (x[X_COORD_CD],'Integer', 'X-coordinate for New York State Plane Coordinate System',
                                       location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'X')))
# Y_COORD_CD 
Y_COORD_CD_check = data.map(lambda x: (x[Y_COORD_CD],'Integer', 'X-coordinate for New York State Plane Coordinate System',
                                       location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'Y')))

# Latitude 
Latitude_check = data.map(lambda x:(x[Latitude],'Float','Latitude coordinate for Global Coordinate System',
                                    location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'LA')))
# Longitude
Longitude_check = data.map(lambda x:(x[Longitude],'Float','Longitude coordinate for Global Coordinate System',
                                    location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'LO')))
#Lat_Lon
Lat_Lon_check = data.map(lambda x:(x[Lat_Lon],'Location','Location in Global Coordinate System',
                                    location_valuecheck(x[X_COORD_CD],x[Y_COORD_CD],x[Latitude],x[Longitude],x[Lat_Lon],'GPS')))


def text_valuecheck(text):
    if text=='':
        return 'NULL'
    else:
        return 'Valid'

# PARKS_NM
PARKS_NM_check = data.map(lambda x:(x[PARKS_NM],'TEXT','Name of NYC park, playground or greenspace',text_valuecheck(x[PARKS_NM])))
# PREM_TYP_DESC
PREM_TYP_DESC_check = data.map(lambda x:(x[PREM_TYP_DESC],'TEXT','Specific description of premises',text_valuecheck(x[PREM_TYP_DESC])))
# LOC_OF_OCCUR_DESC
LOC_OF_OCCUR_DESC_check = data.map(lambda x:(x[LOC_OF_OCCUR_DESC],'TEXT','Specific location of occurrence in or around the premises',text_valuecheck(x[LOC_OF_OCCUR_DESC])))
# HADEVELOPT
HADEVELOPT_check = data.map(lambda x:(x[HADEVELOPT],'TEXT','Name of NYCHA housing development',text_valuecheck(x[HADEVELOPT])))


