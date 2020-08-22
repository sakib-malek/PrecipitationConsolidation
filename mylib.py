import pandas as pd

def readYearData(inputfolder, years): 
	print("Started reading data from 4 years")
	temp2 = []
	for i in range(len(years)):
		#if i == 0:
		temp = (pd.read_fwf(inputfolder+ str(years[i]) +".txt"));
		#else :
		#	data.add(pd.read_fwf("./Dataset/"+ str(years[i]) +".txt"));
		temp2.append(temp)
		print("finished reading ", years[i])
	
	data = pd.concat(temp2, axis=0, ignore_index = True)
	print(len(data))
	data.columns = ['STN','WBAN_PRCP','YEARMODA','TEMP','TEMPF','DEWP',
		'DEWPF','SLP','SLPF','STP','STPF','VISIB','VISIBF','WDSP','WDSPF',
		'MXSPD','GUST','MAX','MIN','PRCP','SNDP','FRSHTT']
	
	print("Started data cleaning")
	data = removeAttribute(data)
	data = removeInvalidRows(data)
	data = changeAttributeType(data)
	print("Ended data cleaning")
	
	print("Started data modification")
	prcpStr = data['PRCP']
	prcpFloat = convertPrcpStringToFloat(prcpStr)
	data['PRCP_FLOAT'] = prcpFloat;
	
	dropYearDate = lambda x: ((x%10000)//100)
	data['YEARMODA'] = dropYearDate(data['YEARMODA']);
	print("Ended data modification")
	
	data = data.drop(columns=['PRCP']);
	
	print("Finished reading data from 4 years")
	return data

def removeInvalidRows(data):
	return data[(data['STN'] != "STN---") & (data['PRCP'] != "99.99")] ;

def removeAttribute(data):
	return data.drop(columns=['TEMP','TEMPF','DEWP','DEWPF','SLP','SLPF',
		'STP','STPF','VISIB','VISIBF','WDSP','WDSPF','MXSPD','GUST','MAX',
		'MIN','SNDP','FRSHTT'])
	
def changeAttributeType(data):
	return data.astype({"STN": str, "WBAN_PRCP": str, "YEARMODA": int,
		"PRCP": str})
	
def convertPrcpStringToFloat(prcpStr):
	prcpFloat = []
	for prcp in prcpStr:
		#if prcp == "99.99":
		#	prcpFloat.append(0.0)
		#	continue
		
		if prcp[-1] in ['I','H']:
			mul = 0;
		elif prcp[-1] in ['D','F','G']:
			mul = 1;
		elif prcp[-1] in ['B','E']:
			mul = 2;
		elif prcp[-1] == 'C':
			mul = 4.0/3.0;
		elif prcp[-1] == 'A':
			mul = 4;
		
		val = mul * float(prcp[:-1])
		prcpFloat.append(val)
		
	return prcpFloat

