import re
from pyproj import Proj,transform

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

def validate_ID(IDstring):
    """
    Checks if the input IDstring is a valid 9-digit ID
    Args:
        IDstring: string to validate as ID
    Return:
        one of three indicator string 'VALID', 'INVALID', 'NULL'
    """
    if len(IDstring)==9:
        return 'VALID'
    elif len(IDstring)==0:
        return 'NULL'
    else:
        return 'INVALID'

def validate_date(datestring, begin=1900, end=2016):
    """
    function that checks if data[colnum] matches coltype date
    Args:
        datestring: string to validate as date
        begin, end: acceptable range for years
    Return:
        one of three indicator string 'VALID', 'INVALID', 'NULL'
    """
    if len(datestring)==0:
        return 'NULL'
    # check if date format
    elif (len(datestring)>0 and (re.match('(\d{2})[/](\d{2})[/](\d{4})$', datestring)!=None)):
        # check date feasibility
        month, day, year = datestring.split('/')
        month, day, year = int(month), int(day), int(year)
        # check year
        if not (begin <= year <= end):
            return 'INVALID'
        # check month
        if not (1 <= month <= 12):
            return 'INVALID'
        # check day
        if month in [1,3,5,7,8,10,12]:
            if not (1 <= day <= 31):
                return 'INVALID'
        elif month in [4,6,9,11]:
            if not (1 <= day <= 30):
                return 'INVALID'
        # leap year
        elif month==2:
            if (year%4==0) and not (1 <= day <= 29):
                return 'INVALID'
            elif (year%4!=0) and not (1 <= day <= 28):
                return 'INVALID'
        return 'VALID'
    else:
        return 'INVALID'

def validate_time(timestring):
    """
    function that checks if data[colnum] matches coltype time
    Args:
        timetsring: string to validate as time
    Return:
        one of three indicator string 'VALID', 'INVALID', 'NULL'
    """
    if len(timestring)==0:
        return 'NULL'
    # check time format
    elif (len(timestring)>0 and (re.match('(\d{2})[:](\d{2})[:](\d{2})$', timestring)!=None)):
        hour, minute, seconds = timestring.split(':')
        hour, minute, seconds = int(hour), int(minute), int(seconds)
        if hour==24 and minute==0 and seconds==0:
            return 'NULL'
        if not (0<=hour<24):
            return 'INVALID'
        if not (0<=minute<=59):
            return 'INVALID'
        if not (0<=seconds<=59):
            return 'INVALID'
        return 'VALID'
    else:
        return 'INVALID'

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
    """
    check whether value in BORO_NM is a valid Borough namd and consistent with the precinct code
    Args:
        borough: string to check as borough
        precinct: string to consider as precinct
    Return:
        one of three indicator string 'VALID', 'INVALID', 'NULL'
    """
    if borough=='':
        return 'NULL'
    else:
        if (borough.upper() in Borough.keys()) and precinct in Borough[borough.upper()]:
            return 'VALID'
        else:
            return 'INVALID'

def precinct_valuecheck(borough,precinct):
    """
    check whether value in ADDR_PCT_CD is a valid Precinct ID and consistent with the Borough
    Args:
        borough: string to check as borough
        precinct: string to consider as precinct
    Return:
        one of three indicator string 'VALID', 'INVALID', 'NULL'
    """
    if precinct=='':
        return 'NULL'
    else:
        if (precinct in precictdict.keys()) and precinct in Borough[borough.upper()]:
            return 'VALID'
        else:
            return 'INVALID'


# geographic imformation match checking
def check_Projection(X_cordi,Y_cordi,latitud, longitude):
    """
    Location information consistency under New York State Plane Coordinate System and Global Coordinate System
    Args:
       X_cordi: X_COORD_CD varible 
       Y_cordi: Y_COORD_CD varible 
       latitud: Latitude varible 
       longitude:  Longitude varible 
    Return:
       Boolean indicator for consistency
    """
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
    """
    Global Coordinate System Location information consistency between latitude, longitude and Lat_Lon
    Args:
       latitud: Latitude varible 
       longitude:  Longitude varible 
       gps_com: Lat_Lon coordinate
    Return:
        Boolean indicator for consistency
    """
    if latitud!='' and longitude!='' and gps_com!='' and '(%s, %s)' % (latitud, longitude) == gps_com:
        return True
    else:
        return False

def location_valuecheck(X_cordi,Y_cordi,latitud, longitude, gps_com, col):
    """
    Location related variable checker. 
    Args:
        X_cordi,Y_cordi,latitud, longitude, gps_com
        col: variable indicator. Possible values: 'X':X_cordi ,'Y': Y_cordi, 'LA':latitud, 'LO':longitude, 'GPS':gps_com
    Return:
        one of three indicator string 'VALID', 'INVALID', 'NULL'
    """
    dict_missing = {'X':X_cordi ,'Y': Y_cordi, 'LA':latitud, 'LO':longitude, 'GPS':gps_com}
    if dict_missing[col]=='':
        return 'NULL'
    else:  
        if (col in ['X','Y'] and check_Projection(X_cordi,Y_cordi,latitud, longitude)) or (col in ['LA','LO'] and check_Projection(X_cordi,Y_cordi,latitud, longitude) and check_Lat_Lon(latitud, longitude,gps_com)) or (col == 'GPS' and check_Lat_Lon(latitud, longitude,gps_com)):
            return 'VALID'
        else:
            return 'INVALID'
def text_valuecheck(text):
    """
    Checks if the input string is empty
    Args:
        text: string to check
    Return:
        one of three indicator string 'VALID', 'INVALID', 'NULL'
    """
    if text=='' or text==' ':
        return 'NULL'
    else:
        return 'VALID'
