import os
import time
import numpy as np
import itertools as it
import pickle
import mlrose
import re
import time
import pandas as pd
from geopy import distance
import math


DATA_ROOT = 'Data\\'

USER_IN = pd.read_csv('USER_INPUT.csv', encoding = 'unicode_escape', delimiter=';')

TRUCK_COMPULSORY_DISTANCE = USER_IN.TRUCK_COMPULSORY_DISTANCE[0]
TRUCK_AVG_SPD = USER_IN.TRUCK_AVG_SPD[0]
BIKE_AVG_SPD = USER_IN.BIKE_AVG_SPD[0]

AVG_UNLOADING_TIME = USER_IN.AVG_UNLOADING_TIME[0]

BIKE_DAILY_FIX = USER_IN.BIKE_DAILY_FIX[0]
BIKE_PER_KM = USER_IN.BIKE_PER_KM[0]
TRUCK_DAILY_FIX = USER_IN.TRUCK_DAILY_FIX[0]
TRUCK_PER_KM = USER_IN.TRUCK_PER_KM[0]

TRUCK_MAX_BILL = USER_IN.TRUCK_MAX_BILL[0]
TRUCK_MIN_BILL = USER_IN.TRUCK_MIN_BILL[0]
BIKE_MAX_BILL = USER_IN.BIKE_MAX_BILL[0]
BIKE_MIN_BILL = USER_IN.BIKE_MIN_BILL[0]

TRUCK_MAX_WEIGHT = USER_IN.TRUCK_MAX_WEIGHT[0]
TRUCK_MIN_WEIGHT = USER_IN.TRUCK_MIN_WEIGHT[0]
BIKE_MIN_WEIGHT = USER_IN.BIKE_MIN_WEIGHT[0]
BIKE_MAX_WEIGHT = USER_IN.BIKE_MAX_WEIGHT[0]

TRUCK_MAX_LINES = USER_IN.TRUCK_MAX_LINES[0]
TRUCK_MIN_LINES = USER_IN.TRUCK_MIN_LINES[0]
BIKE_MIN_LINES = USER_IN.BIKE_MIN_LINES[0]
BIKE_MAX_LINES = USER_IN.BIKE_MAX_LINES[0]

SHOGUN = pd.read_csv(DATA_ROOT + 'Shogun Report Nov 2018.csv', encoding = 'unicode_escape')
CHENNAI = pd.read_csv(DATA_ROOT + 'CHENNAI (6).csv', encoding ='unicode_escape')
CHENNAI = CHENNAI.drop_duplicates(subset='PartyHLLCode', keep = 'first')
xls = pd.ExcelFile(DATA_ROOT+'Dynamic Routing Master (ISWARYAM MARKETING).xls')
RS_Location = pd.read_excel(xls,'RS Location')
Van_Master = pd.read_excel(xls,'Van Master')
Consideration = pd.read_excel(xls,'Local considerations')
LAT_LONG_DETAILS = pd.read_csv('HLL_LATITUDE_LONGITUDE.csv')

USER_RS_CODE = USER_IN.USER_RS_CODE[0]
DATE1 = USER_IN['DATE(MM/DD/YYYY)'][0]
FLAG = 0
RS_HLL = (CHENNAI.PartyHLLCode[CHENNAI.Rscode==USER_RS_CODE]).tolist()
RS_LATITUDE, RS_LONGITUDE = 0, 0
try:
   for index, row in RS_Location.iterrows():
       if USER_RS_CODE == row['RS CODE']:
           print('Finding your orders...')
           FLAG = 1
           RS_LATITUDE = row['Latitude']
           RS_LONGITUDE = row['Longitude']
           time.sleep(2)


except:
   print('Sorry, was unable to find your RS CODE, try entering the correct RS Code again.')

if FLAG == 0:
   print('Sorry, was unable to find your RS CODE, try entering the correct RS Code again.')

LCONSIDERATIONS_DAY = {}
LCONSIDERATIONS_TIME = {}

for index, row in Consideration.iterrows():
   consideration = str(row['Local considerations'])
   if re.search('CLOSED', consideration):
       if re.search('ON', consideration):
           OFF_DAY = consideration.split()[-1]
           MARKET = consideration.split()[0]
           LCONSIDERATIONS_DAY[MARKET] = OFF_DAY
       else:
           OFF_START_TIME = consideration.split()[-3]
           OFF_END_TIME = consideration.split()[-1]
           MARKET = consideration.split()[0]
           TIME = OFF_START_TIME + ' ' + OFF_END_TIME
           LCONSIDERATIONS_TIME[MARKET] = TIME

'''HLL_DIST = {}

try:
   for index, row in LAT_LONG_DETAILS.iterrows():
       if row['PartyHLLCode'] in RS_HLL:
           HLL_LAT = row['Outlet Lat']
           HLL_LONG = row['Outlet Long']
           dist = distance.distance((HLL_LAT,HLL_LONG),(RS_LATITUDE,RS_LONGITUDE))
           HLL_DIST[row['PartyHLLCode']] = float(str(dist).split()[0][:5])
except:
   pass
SORTED_HLL_DIST = {}
for key, value in sorted(HLL_DIST.items(), key = lambda kv:kv[1], reverse=True):
   SORTED_HLL_DIST[key] = value
'''
with open('objdist.pkl', 'rb') as f:
   SORTED_HLL_DIST = pickle.load(f)


NUM_VAN = 0
NUM_BIKE = 0
for i, row in Van_Master.iterrows():
   if re.search('TATA ACE', str(row['Unnamed: 1'])):
       NUM_VAN += int(row['Unnamed: 2'])
   if re.search('BIKE', str(row['Unnamed: 1'])):
       NUM_BIKE += int(row['Unnamed: 2'])


SHOGUN = SHOGUN.loc[SHOGUN['PARTY_HLL_CODE'].isin(RS_HLL)]
SHOGUN = SHOGUN.loc[SHOGUN['Delivery Date'] == DATE1]

TRUCK_COM = {key:value for key, value in SORTED_HLL_DIST.items() if value>TRUCK_COMPULSORY_DISTANCE}
BIKE_COM = {key:value for key, value in SORTED_HLL_DIST.items() if value>TRUCK_COMPULSORY_DISTANCE}

TRUCK_COM_LIST = list(TRUCK_COM.keys())
BIKE_COM_LIST = list(TRUCK_COM.keys())


SHOGUN = SHOGUN.groupby(['PARTY_HLL_CODE','BILL_NUMBER']).sum()
SHOGUN = SHOGUN.reset_index()
SHOGUN = SHOGUN[['PARTY_HLL_CODE','BILL_NUMBER','NET_SALES_WEIGHT_IN_KGS']]
UNIQUE_RS_BILL_DATE = (SHOGUN['PARTY_HLL_CODE']).tolist()

RS_HLL_COORDINATES = CHENNAI.loc[CHENNAI['PartyHLLCode'].isin(UNIQUE_RS_BILL_DATE)]
TRS_HLL_COORDINATES = RS_HLL_COORDINATES.loc[RS_HLL_COORDINATES['PartyHLLCode'].isin(TRUCK_COM)]
BRS_HLL_COORDINATES = RS_HLL_COORDINATES.loc[RS_HLL_COORDINATES['PartyHLLCode'].isin(BIKE_COM)]
TRS_HLL_COORDINATES = TRS_HLL_COORDINATES[['PartyHLLCode','Outlet Lat', 'Outlet Long', 'BeatName']]
BRS_HLL_COORDINATES = BRS_HLL_COORDINATES[['PartyHLLCode','Outlet Lat', 'Outlet Long', 'BeatName']]

coordinates = []

for i, row in TRS_HLL_COORDINATES.iterrows():
   coordinates.append((float(row['Outlet Lat']),float(row['Outlet Long'])))
listt = mlrose.TravellingSales(coords=coordinates)
problem_fit = mlrose.TSPOpt(length = len(coordinates), fitness_fn = listt, maximize=False)
best_state, best_fitness = mlrose.genetic_alg(problem_fit, random_state = 2)

bill_hll = []
for i in best_state:
   lat,long = coordinates[i]
   for index, row in TRS_HLL_COORDINATES.iterrows():
       if row['Outlet Long'] == long and row['Outlet Lat'] == lat:
           for i1,row1 in SHOGUN.iterrows():
               if row1['PARTY_HLL_CODE'] == row['PartyHLLCode']:
                   bill_hll.append(row1['BILL_NUMBER'])
TRUCK_ID = 0
TRUCK_DIST_TOTAL = 0
lat_previous = RS_LATITUDE
long_previous = RS_LONGITUDE
for i, row in SHOGUN.iterrows():
   if row['NET_SALES_WEIGHT_IN_KGS']>TRUCK_MAX_WEIGHT:
       print('TRUCK ID: ' + str(TRUCK_ID) + ' :  BILL NUMBER ' + row['BILL_NUMBER'] + ' to ' + row['PARTY_HLL_CODE'])
       SHOGUN['NET_SALES_WEIGHT_IN_KGS'][i] = row['NET_SALES_WEIGHT_IN_KGS'] - TRUCK_MAX_WEIGHT
       for z, rew in TRS_HLL_COORDINATES.iterrows():
           if rew['PartyHLLCode'] == row['PARTY_HLL_CODE']:
               lat, long = rew['Outlet Lat'], rew['Outlet Long']
               break
       distance_truck = float(str(distance.distance((lat_previous, long_previous), (lat, long))).split()[0][:5])
       print('TRUCK ID: ' + str(TRUCK_ID) + 'DISTANCE' + str(distance_truck))
       TRUCK_ID += 1
       TRUCK_DIST_TOTAL +=distance_truck

TRUCK_LOD_CURR = 0
order_delivery = 0
distance_truck = 0
for j in range(NUM_VAN):
   for i in range(int(len(bill_hll)/2)):
       print('TRUCK ID:'+str(TRUCK_ID) )
       bill_number = bill_hll[i]
       for i, row in SHOGUN.iterrows():
           if row['BILL_NUMBER'] == bill_number:
               hll = row['PARTY_HLL_CODE']
               weight = row['NET_SALES_WEIGHT_IN_KGS']
               TRUCK_LOD_CURR +=weight
               for z,rew in TRS_HLL_COORDINATES.iterrows():
                   if rew['PartyHLLCode'] == hll:
                       lat,long = rew['Outlet Lat'], rew['Outlet Long']
                       break
               distance_tr = float(str(distance.distance((lat_previous,long_previous),(lat,long))).split()[0][:5])
               TRUCK_DIST_TOTAL += distance_tr
               distance_truck +=distance_tr
               lat_previous = lat
               long_previous = long
               order_delivery+=1

               if TRUCK_LOD_CURR > TRUCK_MAX_WEIGHT:
                   i -= 1
                   distance_truck-=distance_tr
                   print('DISTANCE TRAVELLED: '+str(distance_truck))
                   print('TIME TAKEN FOR THIS DELIVERY: '+str(int(order_delivery*AVG_UNLOADING_TIME)+int((distance_truck/TRUCK_AVG_SPD)*60)))
                   distance_truck =0
                   TRUCK_ID +=1
                   order_delivery -=1
                   TRUCK_LOD_CURR=0
                   lat_previous = RS_LATITUDE
                   long_previous = RS_LONGITUDE
                   break
               print("Bill Number " + bill_number + " to " + hll+' weight ' + str(weight))
   break

TRUCK_COST = (TRUCK_DAILY_FIX*(TRUCK_ID+1)) + (TRUCK_PER_KM*TRUCK_DIST_TOTAL)

print('TRUCK CHARGES FOR TODAY : Rs. ' + str(TRUCK_COST))




coordinates = []

for i, row in BRS_HLL_COORDINATES.iterrows():
   coordinates.append((float(row['Outlet Lat']),float(row['Outlet Long'])))
listt = mlrose.TravellingSales(coords=coordinates)
problem_fit = mlrose.TSPOpt(length = len(coordinates), fitness_fn = listt, maximize=False)
best_state, best_fitness = mlrose.genetic_alg(problem_fit, random_state = 2)

bill_hll = []
for i in best_state:
   lat,long = coordinates[i]
   for index, row in TRS_HLL_COORDINATES.iterrows():
       if row['Outlet Long'] == long and row['Outlet Lat'] == lat:
           for i1,row1 in SHOGUN.iterrows():
               if row1['PARTY_HLL_CODE'] == row['PartyHLLCode']:
                   bill_hll.append(row1['BILL_NUMBER'])
BIKE_ROUND = 0
BIKE_DIST_TOTAL = 0
lat_previous = RS_LATITUDE
long_previous = RS_LONGITUDE
for i, row in SHOGUN.iterrows():
   if row['NET_SALES_WEIGHT_IN_KGS']>BIKE_MAX_WEIGHT:
       print('BIKE ROUND: ' + str(BIKE_ROUND) + ' :  BILL NUMBER ' + row['BILL_NUMBER'] + ' to ' + row['PARTY_HLL_CODE'])
       SHOGUN['NET_SALES_WEIGHT_IN_KGS'][i] = row['NET_SALES_WEIGHT_IN_KGS'] - BIKE_MAX_WEIGHT
       for z, rew in BRS_HLL_COORDINATES.iterrows():
           if rew['PartyHLLCode'] == row['PARTY_HLL_CODE']:
               lat, long = rew['Outlet Lat'], rew['Outlet Long']
               break
       distance_truck = float(str(distance.distance((lat_previous, long_previous), (lat, long))).split()[0][:5])
       print('BIKE ROUND: ' + str(TRUCK_ID) + 'DISTANCE' + str(distance_truck))
       BIKE_ROUND += 1
       BIKE_DIST_TOTAL +=distance_truck

BIKE_LOD_CURR = 0
distance_bike = 0
for j in range(NUM_VAN):
   for i in range(int(len(bill_hll)/2)):
       print('BIKE ROUND:'+str(BIKE_ROUND) )
       bill_number = bill_hll[i]
       for i, row in SHOGUN.iterrows():
           if row['BILL_NUMBER'] == bill_number:
               hll = row['PARTY_HLL_CODE']
               weight = row['NET_SALES_WEIGHT_IN_KGS']
               BIKE_LOD_CURR +=weight
               for z,rew in BRS_HLL_COORDINATES.iterrows():
                   if rew['PartyHLLCode'] == hll:
                       lat,long = rew['Outlet Lat'], rew['Outlet Long']
                       break
               distance_tr = float(str(distance.distance((lat_previous,long_previous),(lat,long))).split()[0][:5])
               BIKE_DIST_TOTAL += distance_tr
               distance_bike +=distance_tr
               lat_previous = lat
               long_previous = long

               if BIKE_LOD_CURR > BIKE_MAX_WEIGHT:
                   i -= 1
                   print('DISTANCE TRAVELLED: '+str(distance_bike))
                   distance_bike =0
                   BIKE_ROUND +=1
                   BIKE_LOD_CURR=0
                   lat_previous = RS_LATITUDE
                   long_previous = RS_LONGITUDE
                   break
               print("Bill Number " + bill_number + " to " + hll+' weight ' + str(weight))
   break
BIKE_RUNTIME = math.ceil((BIKE_DIST_TOTAL/BIKE_AVG_SPD) + (AVG_UNLOADING_TIME*(BRS_HLL_COORDINATES.shape[0])))
NUMBER_OF_BIKES = math.ceil(BIKE_RUNTIME/4800)
BIKE_COST = (BIKE_DAILY_FIX*(NUMBER_OF_BIKES)) + (BIKE_PER_KM*BIKE_DIST_TOTAL)


print('TOTAL BIKE RUNTIME: ' + str(BIKE_RUNTIME))
print('BIKE CHARGES FOR TODAY : Rs. ' + str(BIKE_COST))
print('THROUGH ' + str(NUMBER_OF_BIKES) + ' BIKES')


for i in range(5):
    os.system('cls')
    time.sleep(1)

    print('NUMBER OF TRUCKS: ' + str(TRUCK_ID+1))
    print('NUMBER OF BIKES: ' + str(NUMBER_OF_BIKES))
    print('TOTAL COST: ' + str(BIKE_COST+TRUCK_COST))
    time.sleep(1)
