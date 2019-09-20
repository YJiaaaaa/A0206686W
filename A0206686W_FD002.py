# -*- coding: utf-8 -*-
"""
This python coding pushes data from train_FD002.txt to the aws platform.

@author: A0206686W
"""

# Import neccessary packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import time
import json

# A random programmatic shadow client ID.
SHADOW_CLIENT = "myShadowClient"

# The unique hostname that &IoT; generated for this device.
HOST_NAME = "awvq64v3xprq9-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;,
# which you have already saved onto this device.
ROOT_CA = "AmazonRootCA1.pem"

# The relative path to your private key file that
# &IoT; generated for this device, which you
# have already saved onto this device.
PRIVATE_KEY = "b3ba3aedc6-private.pem.key"

# The relative path to your certificate file that
# &IoT; generated for this device, which you 
# have already saved onto this device.
CERT_FILE = "b3ba3aedc6-certificate.pem.crt.txt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "A0206686W_DataTwo"

# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
    print()
    print('UPDATE: $aws/things/' + SHADOW_HANDLER + '/shadow/update/#')
    print("payload = " + payload)
    print("responseStatus = " + responseStatus)
    print("token = " + token)
        
# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY, CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(SHADOW_HANDLER,
                                                            True)

# Open the first file named 'train_FD001.txt'.
# and read data from into a matrix.
dataMat = []
with open('train_FD002.txt', 'r') as file:
    for line in file.readlines():
        linestrlist = line.strip().split(' ')
        linelist = list(map(float, linestrlist))
        dataMat.append(linelist[0:26])
engineData = np.array(dataMat)

# Name the original martix column.
osNames = ['os' + str(i) for i in range(1,4)]
sensorNames = ['sensor' + str(i) for i in range(1,22)]
columnNames = ['id', 'cycle'] + osNames + sensorNames
engineDF = pd.DataFrame(engineData, columns=columnNames)
engineDF.id = engineDF.id.map(lambda x: 'FD002_' + str(int(x)))

# Overwrite column 'id' as 'FD001'+id.
# Add one column 'timestamp' as timestamp in UTC.
# and another one column 'MaticID'.
# Publish all data to the thing 'DataOne' under AWS IoT platform.
# For the testing convenience,
# only the first ten rows of data are published,
# and the publishing rate is set to 2 seconds per row.
for i in range(10):
    everyRow = engineDF.iloc[i]
    # obtain the local UTC 
    utcSet = datetime.utcnow().replace(tzinfo=timezone.utc)
    utcLocal = utcSet.astimezone(timezone(timedelta(hours=8)))
    # add one more columns 'timestamp' & 'MatricID'
    newRow = everyRow.append(pd.Series(['UTC ' + str(utcLocal), 'A0206686W'],
                                       index = ['timestamp', 'MatricID']))
    newRow = newRow.to_dict()
    jsonFile = {"state": {"reported": {"data": newRow}}}
    jsonStr = json.dumps(jsonFile)
    #print(repr(jsonStr))
    myDeviceShadow.shadowUpdate(jsonStr, myShadowUpdateCallback, 5)
    time.sleep(2)
    
# Wait for this test value to be added.
time.sleep(1)
