#!/usr/bin/python

import os
import sys
import argparse
from kazoo.client import KazooClient

LOCAL_ZKHOST='localhost:2181'
QA_ZKHOST='10.41.1.18:2181,10.41.1.19:2181,10.41.1.20:2181'
PROD_ZKHOST='bodcprodvzk217.prod.lattice.local:2181,bodcprodvzk218.prod.lattice.local:2181,bodcprodvzk219.prod.lattice.local:2181'
LOCAL_ENV = 'Default'
QA_ENV = 'QA'
PROD_ENV = 'Production'

def main(argv):
    envDic = {LOCAL_ENV:LOCAL_ZKHOST, QA_ENV:QA_ZKHOST, PROD_ENV:PROD_ZKHOST}

    parsedResult = parseArguments()
    print 'Environment is', parsedResult.env, ', Feature Flags are' , parsedResult.featureFlags, ', File for tenant information is', parsedResult.f, ', all tenants is', parsedResult.allTenants
    print envDic
    if parsedResult.env not in envDic.keys():
        raise ValueError('The parsedResult does not have valid environment variable')
    zk = KazooClient(hosts=envDic.get(parsedResult.env))
    zk.start()


    # Operate to all the tenants in that environment
    if parsedResult.allTenants:
        print 'Searching all the znodes to update the feature flags'
        contractZnode = os.path.join("/Pods", parsedResult.env, "Contracts")
        contracts = zk.get_children(contractZnode)
        for contract in contracts:
            updateFeatureFlag(contract, parsedResult.env, parsedResult.featureFlags, zk)
    # Get tenant/contract information from the file
    else:
        fileName = parsedResult.f
        with open(fileName) as f:
            for line in f:
                contract = line.strip()
                updateFeatureFlag(contract, parsedResult.env, parsedResult.featureFlags, zk)

def updateFeatureFlag(contract, env, newfeatureFlags, zk):
    znode = os.path.join("/Pods", env, "Contracts", contract, "Tenants", contract, "Spaces", "Production/feature-flags.json")
    if (zk.exists(znode)):
        print "Current znode is: ", znode
        existingFeatureFlags = zk.get(znode)[0]
        combinedFf = combineFeatureFlags(existingFeatureFlags, newfeatureFlags)
        zk.set(znode, combinedFf)
    else:
        print "znode ", znode, "does not exist"


def combineFeatureFlags(existingFeatureFlags, newFeatureFlags):
    print "existing feature flags are ", existingFeatureFlags
    print "new feature flags are ", newFeatureFlags
    existingContents = findBetween(existingFeatureFlags, '{', '}')
    newFfList = newFeatureFlags.split(',')
    for ff in newFfList:
        featureFlag = ff.split(':')[0]
        featureFlagValue = ff.split(':')[1]
        #Create new feature flag value
        if featureFlag not in existingFeatureFlags:
            print "existingContents is " + existingContents
            newFeatureFlag = '"'+featureFlag+'":'+featureFlagValue
            print "newFeatureFlag is ", newFeatureFlag
            if not existingContents.strip() or existingFeatureFlags == '{}':
                existingContents = newFeatureFlag 
            else:
                existingContents = existingContents + ',' + newFeatureFlag 
        #Overwrite the existing featureFlag value
        else:
            print 'Overwriting the feature flag ', featureFlag
            # add the length for the colon
            start = existingContents.index(featureFlag) + len(featureFlag) + 2
            try:
                end = existingContents.index(',', start)
            except ValueError:
                end = len(existingContents) 

            existingContents = existingContents[:start] + featureFlagValue + existingContents[end:]
    return '{' + existingContents  + '}'

def parseArguments():
    parser = argparse.ArgumentParser(description='Process the input file for tenant information and the feature flag values')
    parser.add_argument("-env", help='Specifiy the environment info')
    parser.add_argument("-f", help='Specifiy the file for tenant info. The feature flags values should be indicated in the format of key1:value1,key2:value2. \
        Note that there is no spaces between different key value pairs')
    parser.add_argument("-featureFlags", help="Specify the feature flag values")
    parser.add_argument("-all", "--allTenants", action="store_true", help="Apply to all the tenants in the environment")
    result = parser.parse_args()
    return result

def findBetween(s, first, last):
    start = s.index(first) + len(first)
    end = s.index(last, start)
    return s[start:end]

if __name__ == '__main__':
    main(sys.argv[1:])