#!/usr/bin/python

#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import os,sys, datetime, time, requests, re, csv, operator
from liaison import *


filePath = os.path.abspath('..')+'\\resources\\'
fileName = 'tenant_url.csv'
tenantFileName = filePath + fileName
resultFileName = filePath + 'resultSettings.csv'
resultFileRank = 'resultSettingsRank.csv'
resultFiledup = 'resultSettingsDup.csv'


def GetDLTenantSettings(resultFileName):
  tenants = []

  with open(tenantFileName) as tenantFile:
    for line in tenantFile:
      cols = line.strip().split(',')
      tenants.append([cols[0], cols[1]])

  with open(resultFileName, mode='w') as resultFile:
    resultFile.write('TenantName,TenantURL,VisiDBName,VisiDBServerLocation\n')

    for t in tenants:
      print '{0}...'.format(t[0])
      try:
        conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=t[0])

        VisiDBName = conn_mgr.getDLTenantSettings()[5]["Value"]
        VisiDBServerLocation = conn_mgr.getDLTenantSettings()[4]["Value"]
        print VisiDBName, VisiDBServerLocation
        resultFile.write('{0},{1},{2},{3}\n'.format(t[0], t[1], VisiDBName, VisiDBServerLocation))

      except requests.exceptions.SSLError:
        ## Not on a PROD DataLoader
        pass
      except TenantNotFoundAtURL:
        ## Not on a PROD DataLoader
        pass
      except requests.exceptions.ConnectionError:
        ## EndpointError
        print  'HTTP Endpoint did not return the expected response'
        VisiDBName = 'Unknown'
        VisiDBServerLocation = 'Unknown'
        resultFile.write('{0},{1},{2},{3}\n'.format(t[0], t[1], VisiDBName, VisiDBServerLocation))

      except XMLStringError:
        print  'No connection could be made because the target machine actively refused'
        VisiDBName = 'Unknown'
        VisiDBServerLocation = 'Unknown'
        resultFile.write('{0},{1},{2},{3}\n'.format(t[0], t[1], VisiDBName, VisiDBServerLocation))

    resultFile.close()

# 1. Get DL Tenant Settings
GetDLTenantSettings(resultFileName)

# 2. Sort the Result DL Settings by VisiDBName and VisiDBServerLocation
resultFile = open(resultFileName, 'r')
header = resultFile.readline()
fileset = set()
for eachline in resultFile:
  fileset.add(eachline)
resultFile.close()
filelist = [each.split(",") for each in fileset]
filelist.sort(key=lambda x: (x[2], x[3]), reverse=False)

with open(filePath + resultFileRank, "wb") as f:
  writer = csv.writer(f)
  writer.writerow(header[:-1].split(","))
  for row in filelist:
    writer.writerow(row[:(len(row) - 1)] + [row[len(row) - 1][:-1]])
f.close()

# 3. Dedupliacte the Result
TenantDict = {}
with open(filePath + resultFileRank, "r") as f:
  header = f.readline()
  for each in f:
    if each.split(",")[2] != "Unknown" and each.split(",")[3] != "Unknown\n":
      if "".join(each[:-1].split(",")[2:]) in TenantDict.keys():
        TenantDict["".join(each[:-1].split(",")[2:])].append(each[:-1])
      else:
        TenantDict["".join(each[:-1].split(",")[2:])] = [each[:-1]]
f.close()

# 4. Print and output the Tenant with same backend
with open(filePath + resultFiledup, "w") as f:
  f.write(header)
  for i in TenantDict.keys():
    if len(TenantDict[i]) > 1:
      print  TenantDict[i]
      for each in TenantDict[i]:
        f.write(each + '\n')
f.close()
os.remove(filePath + resultFileRank)
