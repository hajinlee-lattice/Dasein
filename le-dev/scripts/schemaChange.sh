#!/usr/bin/python

import sys
import os
import glob
import re
import argparse

DROP_PRIMARY_KEY_PATTERN = "ALTER TABLE \[dbo\].\[(.+)\] DROP CONSTRAINT \[PK.+\]"
DROP_DEFAULT_KEY_PATTERN = "ALTER TABLE \[dbo\].\[(.+)\] DROP CONSTRAINT \[DF.+\]"
DROP_UNIQUE_KEY_PATTERN = "ALTER TABLE \[dbo\].\[(.+)\] DROP CONSTRAINT \[UQ.+\]"
ADD_PRIMARY_KEY_PATTERN = "ALTER TABLE \[dbo\].\[(.+)\] ADD CONSTRAINT \[PK.+\]"
ADD_DEFAULT_KEY_PATTERN = "ALTER TABLE \[dbo\].\[(.+)\] ADD CONSTRAINT \[DF.+\]"
ADD_UNIQUE_KEY_PATTERN = "ALTER TABLE \[dbo\].\[(.+)\] ADD CONSTRAINT \[UQ.+\]"
NUMBER = 'number'
CONTENT = "contents"
DELIMITER = "GO\r\n"


def main(argv):
    parsedResult = parseArguments()
    inputFileName = parsedResult.inputFile

    keyPatterns = [DROP_PRIMARY_KEY_PATTERN, DROP_DEFAULT_KEY_PATTERN, DROP_UNIQUE_KEY_PATTERN, ADD_PRIMARY_KEY_PATTERN, ADD_DEFAULT_KEY_PATTERN, ADD_UNIQUE_KEY_PATTERN]
    keyChanges = {}

    outputFileName = 'schemaChangeResult.sql'
    if os.path.isfile(outputFileName):
        print 'romving outputFile'
        os.remove(outputFileName)

    inputFile = open(inputFileName, "r")
    outputFile = open(outputFileName, "a")
    inputString = inputFile.read()
    splitWithGos = inputString.split(DELIMITER)

    for eachSplit in splitWithGos:
        firstLine = eachSplit.strip().split("\n")[0]
        findMatch = False
        for pattern in keyPatterns:
            matchObj = re.match(pattern, firstLine, flags=0)
            if matchObj:
                processKeyPattern(matchObj.group(1), pattern, eachSplit, keyChanges)
                findMatch = True
                break;
        if not findMatch:
            outputFile.write(eachSplit)
            outputFile.write(DELIMITER)

    processKeyChanges(keyChanges)
    print keyChanges
    writeKeyChangesToFile(keyChanges, outputFile)

    inputFile.close()
    outputFile.close()

# parse the argument to get the input and output file
def parseArguments():
    parser = argparse.ArgumentParser(description='Process the input and output sql files for schema change')
    parser.add_argument('-i', action='store', dest='inputFile', help='Specifiy the input fileName')
    result = parser.parse_args()
    return result

def processKeyChanges(keyChanges):
    for table in keyChanges.keys():
        if keyChanges[table].has_key(DROP_PRIMARY_KEY_PATTERN) and keyChanges[table].has_key(ADD_PRIMARY_KEY_PATTERN):
            if keyChanges[table][DROP_PRIMARY_KEY_PATTERN][NUMBER] == keyChanges[table][ADD_PRIMARY_KEY_PATTERN][NUMBER]:
                del keyChanges[table][DROP_PRIMARY_KEY_PATTERN]
                del keyChanges[table][ADD_PRIMARY_KEY_PATTERN]
        if keyChanges[table].has_key(DROP_DEFAULT_KEY_PATTERN) and keyChanges[table].has_key(ADD_DEFAULT_KEY_PATTERN):
            if keyChanges[table][DROP_DEFAULT_KEY_PATTERN][NUMBER] == keyChanges[table][ADD_DEFAULT_KEY_PATTERN][NUMBER]:
                del keyChanges[table][DROP_DEFAULT_KEY_PATTERN]
                del keyChanges[table][ADD_DEFAULT_KEY_PATTERN]
        if keyChanges[table].has_key(DROP_UNIQUE_KEY_PATTERN) and keyChanges[table].has_key(ADD_UNIQUE_KEY_PATTERN):
            if keyChanges[table][DROP_UNIQUE_KEY_PATTERN][NUMBER] == keyChanges[table][ADD_UNIQUE_KEY_PATTERN][NUMBER]:
                del keyChanges[table][DROP_UNIQUE_KEY_PATTERN]
                del keyChanges[table][ADD_UNIQUE_KEY_PATTERN]
        if not keyChanges[table]:
            del keyChanges[table]

def writeKeyChangesToFile(keyChanges, outputFile):
    for table in keyChanges.keys():
        if keyChanges[table]:
            for pattern in keyChanges[table].keys():
                for content in keyChanges[table][pattern][CONTENT]:
                    outputFile.write(str(content))
                    outputFile.write(DELIMITER)


# process the line to see if it matches with any key pattern
def processKeyPattern(table, pattern, content, keyChanges):
    # update the inner dictionary 
    # table:{pattern1: {number:#1, contents: ["$$$\n", "$$$\n"]}, pattern2: {number:#2, contents: ["$$$\n", "$$$\n"]}}
    if keyChanges.has_key(table) and keyChanges[table].has_key(pattern):
        number = keyChanges[table][pattern][NUMBER]
        number += 1
        contents = keyChanges[table][pattern][CONTENT]
        contents.append(content)
        keyChanges[table][pattern]={NUMBER:number, CONTENT:contents}
    elif keyChanges.has_key(table) and not keyChanges[table].has_key(pattern):
        patternDic ={pattern:{NUMBER:1, CONTENT:[content]}}
        mergedDic = dict(keyChanges[table].items() + patternDic.items())
        keyChanges[table] = mergedDic
    else:
        patternDic ={pattern:{NUMBER:1, CONTENT:[content]}}
        keyChanges[table] = patternDic


if __name__ == "__main__":
       main(sys.argv[1:])