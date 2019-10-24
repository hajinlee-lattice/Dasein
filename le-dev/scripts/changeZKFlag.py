#!/usr/bin/env python

###############################################
# Usage: changeZKFlag.py -t diagnostic -env Default -tenant -flags QueryLogging:true
###############################################

import argparse
import os
import sys

from kazoo.client import KazooClient

LOCAL_ZKHOST = 'localhost:2181'
QA_ZKHOST = 'internal-zookeeper-1213348105.us-east-1.elb.amazonaws.com:2181'
PROD_ZKHOST = 'internal-Zookeeper-227174924.us-east-1.elb.amazonaws.com:2181'
LOCAL_ENV = 'Default'
QA_ENV = 'QA'
PROD_ENV = 'Production'
envDic = {LOCAL_ENV: LOCAL_ZKHOST, QA_ENV: QA_ZKHOST, PROD_ENV: PROD_ZKHOST}


def main(argv):
    parsedResult = parseArguments()

    if parsedResult.env not in envDic.keys():
        print("Invalid -env flag. Valid values:", envDic)
        raise ValueError('The parsedResult does not have valid environment variable')
    else:
        print('Environment is', parsedResult.env, ', Diagnostic Flags are', parsedResult.flags,
              ', File for tenant information is', parsedResult.f, ', all tenants is', parsedResult.allTenants)
    zk = KazooClient(hosts=envDic.get(parsedResult.env))
    zk.start()

    # Operate to all the tenants in that environment
    if parsedResult.allTenants:
        print('Searching all the znodes to update the feature flags')
        contractZnode = os.path.join("/Pods", parsedResult.env, "Contracts")
        contracts = zk.get_children(contractZnode)
        for contract in contracts:
            updateFlag(contract, parsedResult.type, parsedResult.env, parsedResult.flags, zk)
    # Get tenant/contract information from the file
    elif parsedResult.tenant:
        updateFlag(parsedResult.tenant, parsedResult.env, parsedResult.type, parsedResult.flags, zk)
    else:
        fileName = parsedResult.f
        with open(fileName) as f:
            for line in f:
                contract = line.strip()
                updateFlag(contract, parsedResult.env, parsedResult.type, parsedResult.flags, zk)
    zk.stop()


def updateFlag(contract, env, type, newFlags, zk):
    if type == "diagnostic":
        updateDiagnosticFlag(contract, env, newFlags, zk)
    else:
        updateFeatureFlag(contract, env, newFlags, zk)


def updateDiagnosticFlag(contract, env, newFlags, zk):
    flag = newFlags.split(':')[0]
    val = newFlags.split(':')[1].encode('UTF-8')
    znode = os.path.join("/Pods", env, "Contracts", contract, "Tenants", contract, "Spaces",
                         "Production/Services/CDL")
    if (zk.exists(znode)):
        znode = os.path.join(znode, "Diagnostics", flag)
        if (zk.exists(znode)):
            existingFlag = zk.get(znode)[0]
            # print("Current znode is: ", znode, ":", existingFlag)
            if existingFlag != val:
                # print("setting to: ", val)
                zk.set(znode, val, -1)
            # else:
            # print("already set to:", existingFlag)
        else:
            print("znode ", znode, "does not exist. Creating it.")
            zk.create(znode, val, None, False, False, True)
    else:
        print("Could not find", znode, "\nDoing nothing.")
    print(flag, "set to", zk.get(znode)[0], "for", contract)


def updateFeatureFlag(contract, env, newfeatureFlags, zk):
    znode = os.path.join("/Pods", env, "Contracts", contract, "Tenants", contract, "Spaces",
                         "Production/feature-flags.json")
    if (zk.exists(znode)):
        existingFeatureFlags = zk.get(znode)[0]
        print("Current znode is: ", znode, ". Existing feature flags are: ", existingFeatureFlags)
        combinedFf = combineFeatureFlags(existingFeatureFlags, newfeatureFlags)
        zk.set(znode, combinedFf)
        doubleCheckFeatureFlags(zk, znode, existingFeatureFlags, newfeatureFlags)
    else:
        print("znode ", znode, "does not exist")
        sys.exit(1)


def doubleCheckFeatureFlags(zk, znode, existingFeatureFlags, newflags):
    combinedFeatureFlagsContents = findBetween(zk.get(znode)[0], '{', '}')
    combinedFeatureFlagsContentsList = combinedFeatureFlagsContents.split(',')

    combinedDic = {}
    for combinedFeatureFlagsContent in combinedFeatureFlagsContentsList:
        flag = combinedFeatureFlagsContent.split(':')[0].strip()
        value = combinedFeatureFlagsContent.split(':')[1].strip()
        combinedDic[flag] = value

    existingContents = findBetween(existingFeatureFlags, '{', '}')
    if existingContents:
        existingContentsList = existingContents.split(',')
        for existingContent in existingContentsList:
            flag = existingContent.split(':')[0].strip()
            if not combinedDic.has_key(flag):
                print("Error! The ", flag, " is not in the combined value!")
                sys.exit()

    newflagsContentsList = newflags.split(',')
    for newflagsContent in newflagsContentsList:
        flag = '"' + newflagsContent.split(':')[0] + '"'
        value = newflagsContent.split(':')[1].strip()
        if not combinedDic.has_key(flag):
            print("Error! The ", flag, " is not in the combined value!")
            sys.exit()
        else:
            if combinedDic[flag] != value:
                print("Error! The ", value, " does not match with ", combinedDic[flag], " for the flag of ", flag)
                sys.exit()


def combineFeatureFlags(existingFeatureFlags, newFeatureFlags):
    # print("existing feature flags are ", existingFeatureFlags)
    # print("new feature flags are ", newFeatureFlags)
    existingContents = findBetween(existingFeatureFlags, '{', '}')
    newFfList = newFeatureFlags.split(',')
    for ff in newFfList:
        featureFlag = ff.split(':')[0]
        featureFlagValue = ff.split(':')[1]
        # Create new feature flag value
        if featureFlag not in existingFeatureFlags:
            # print("existingContents is " + existingContents)
            newFeatureFlag = '"' + featureFlag + '":' + featureFlagValue
            # print("newFeatureFlag is ", newFeatureFlag)
            if not existingContents.strip():
                existingContents = newFeatureFlag
            else:
                existingContents = existingContents + ',' + newFeatureFlag
        # Overwrite the existing featureFlag value
        else:
            print('Overwriting the feature flag ', featureFlag)
            # add the length for the colon
            start = existingContents.index(featureFlag) + len(featureFlag) + 2
            try:
                end = existingContents.index(',', start)
            except ValueError:
                end = len(existingContents)

            existingContents = existingContents[:start] + featureFlagValue + existingContents[end:]
    return '{' + existingContents + '}'


def parseArguments():
    parser = argparse.ArgumentParser(description='Process the input file for tenant information and the flag values')
    parser.add_argument("-env", help='Specifiy an environment: ' + ', '.join(envDic.keys()))
    parser.add_argument("-f", help='Specify a file listing Tenants to modify with one Tenant name per line.')
    parser.add_argument("-flags", help="Specify pair(s) of flag:value[,flag:value]. The feature flags values \
     should be indicated in the format of key1:value1,key2:value2. \
     Note that there are no spaces between different key:value pairs")
    parser.add_argument("-all", "--allTenants", action="store_true", help="Apply to all the tenants in the environment")
    parser.add_argument("-tenant", help="Name of the Tenant to update. This takes precedence over a -f file.")
    parser.add_argument("-t", "--type", help="Flag type: diagnostic or feature. (default: feature)")
    result = parser.parse_args()
    return result


def findBetween(s, first, last):
    if not s.strip():
        return s
    start = s.index(first) + len(first)
    end = s.index(last, start)
    return s[start:end]


if __name__ == '__main__':
    main(sys.argv[1:])
