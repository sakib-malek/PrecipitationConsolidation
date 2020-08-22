from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd
from mylib import *
import gc
import time
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-i","---input",help="input folder location", type=str)
parser.add_argument("-o","---output",help="output folder location", type=str)
args = parser.parse_args()

inputfolder = "./Dataset/"
outputfolder = "Output/"

if args.input not in  [None, ""]:
	inputfolder = args.input
	print("got input", args.input)
	
if args.output not in  [None, ""]:
	outputfolder = args.output
	print("got output", args.output)

print("\n\ni/o i=", args.input, "o=", args.output)	

SparkContext.setSystemProperty('spark.executor.memory', '12g')
sc = SparkContext("local", "TestApp")
sqlContext = SQLContext(sc)

years = [2006,2007,2008,2009]
#,2008,2009

print("printing from testapp from spark")

start_time = time.time()

weatherStations = pd.read_csv(inputfolder + "WeatherStationLocations.csv");

weatherStations = weatherStations.drop(columns=['STATION NAME','ELEV(M)',
	'BEGIN','END'])

weatherStations.astype({"USAF": str, "WBAN": str, "CTRY": str,
	"STATE": str, "LAT": float, "LON": float})


weatherStationsInUS = weatherStations[weatherStations['CTRY'] == "US"];
states = weatherStationsInUS['STATE'].unique();
states = list(states)
states = states[1:]


schemaStation = StructType([StructField('USAF', StringType()),StructField('WBAN', StringType()),
	StructField('CTRY', StringType()), StructField('STATE', StringType()),
	StructField('LAT',DoubleType()), StructField('LON',DoubleType())])


stationDataset = sqlContext.createDataFrame(weatherStationsInUS, schemaStation)
stationDataset.registerTempTable("weather")
#print(stationDataset)

#exit()

data = readYearData(inputfolder,years);

schemaWeather = StructType([StructField('STN', StringType()),StructField('WBAN_PRCP', StringType()),
	StructField('YEARMODA',ShortType()), StructField('PRCP_FLOAT',DoubleType())])


weatherDataset = sqlContext.createDataFrame(data, schemaWeather)
weatherDataset.registerTempTable("precp")


fileoutput = open(outputfolder+ "output.txt", 'w')
sys.stdout = fileoutput

data_read_time = time.time();
print("\nTime take to read data: %s secs\n" %(data_read_time-start_time))

	
#	statewiseData = sqlContext.sql("""SELECT STATE,AVG(PRCP_FLOAT),YEARMODA
#		FROM precp JOIN weather ON STN = USAF
#		GROUP BY STATE, YEARMODA
#		ORDER BY YEARMODA ASC""")
	
	
#	print(state, statewiseData.collect())
#	statewiseMap[state] = statewiseData.collect()

statewiseData = sqlContext.sql("""SELECT STATE,YEARMODA,AVG(PRCP_FLOAT)
		FROM precp JOIN weather ON STN = USAF AND WBAN = WBAN_PRCP
		GROUP BY STATE, YEARMODA""")

#del data
#del weatherDataset
#del stationDataset
#gc.collect()

#sqlContext.clearCache()

tuples = []

#print(statewiseData.collect())

#exit()

for st,mth,ap in statewiseData.collect():
	tempTuple = (st, mth, ap)
	tuples.append(tempTuple)
	
print(tuples)

schemaInter = StructType([StructField('STATE', StringType()),
	StructField('MONTH', IntegerType()), StructField('PRCP',DoubleType())])

interDataset = sqlContext.createDataFrame(tuples, schemaInter)
interDataset.registerTempTable("inter")

#for state in states:
#	interData = sqlContext.sql("""SELECT *
#		FROM inter 
#		WHERE PRCP = (SELECT MAX(PRCP) FROM inter WHERE STATE = '""" + state + """')
#		AND STATE = '""" + state + """' LIMIT 1""");

#	print(state, interData.collect())
#	interData = sqlContext.sql("""SELECT *
#		FROM inter 
#		WHERE PRCP = (SELECT MIN(PRCP) FROM inter WHERE STATE = '""" + state + """')
#		AND STATE = '""" + state + """' LIMIT 1""");
#	print(state, interData.collect())
	
print("\n\n")

interData = sqlContext.sql("""SELECT STATE, (MAX(PRCP) - MIN(PRCP)) as DIFF_PRCP, MAX(PRCP) as MAX_PRCP, MIN(PRCP) as MIN_PRCP
		FROM inter 
		GROUP BY STATE
		ORDER BY DIFF_PRCP asc""");
#print(interData.collect())
sortedData = interData.collect();

for stateData in sortedData:
	print(stateData)

query_time = time.time();
print("\nTime taken to run queries: %s secs\n" %(query_time-data_read_time))
print("\nTotal time taken: %s secs\n" %(query_time-start_time))


fileoutput.close()	
		
