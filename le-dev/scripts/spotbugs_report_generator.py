from __future__ import print_function

import argparse
import os
import xml.dom.minidom as xml
from collections import Counter

ROOT_PROJECT_PATH_ENV = 'WSHOME'
TARGET_DIR = 'target'
SPOTBUGS_RESULT_FILE = 'spotbugsXml.xml'
AGGREGATED_REPORT_FILE = 'aggregated_spotbugs_report.txt'

def main():
    check()
    args = getArgs()
    proj_dir = os.environ[ROOT_PROJECT_PATH_ENV]
    if args.project:
        proj_dir = proj_dir + "/le-" + args.project
    projectList = findAllXmls(proj_dir)
    totalNumBugs = 0
    totalNumFiles = 0
    totalPatternCntr = Counter()
    totalCodeCntr = Counter()
    totalCategoryCntr = Counter()
    for numBugs, projectName, patternCntr, codeCntr, categoryCntr, fileCntrs in projectList:
        totalNumBugs += numBugs
        totalPatternCntr += patternCntr
        totalCodeCntr += codeCntr
        totalCategoryCntr += categoryCntr
        totalNumFiles += len(fileCntrs)
    report = "%s/%s/%s" % (proj_dir, TARGET_DIR, AGGREGATED_REPORT_FILE)
    print("Writing aggregated report to %s" % report)
    with open(report, 'w') as fout:
        fout.write("\n%d bugs in %d files in total\n" % (totalNumBugs, totalNumFiles))
        fout.write(printCounters(totalPatternCntr, totalCodeCntr, totalCategoryCntr))
        for numBugs, projectName, patternCntr, codeCntr, categoryCntr, fileCntrs in projectList:
            numFiles = len(fileCntrs)
            fout.write("\n%d bugs in %d files in project %s\n" % (numBugs, numFiles, projectName))
            fout.write(printCounters(patternCntr, codeCntr, categoryCntr))
        for numBugs, projectName, patternCntr, codeCntr, categoryCntr, fileCntrs in projectList:
            fout.write("\ndetails for project %s\n" % projectName)
            for filePath, fileDetail in fileCntrs.items():
                fout.write(" - %d bugs in file %s\n" % (fileDetail['bugs'], filePath))
    if args.verbose:
        with open(report, 'r') as fin:
            print(fin.read())


def check():
    # check required environment variables
    if ROOT_PROJECT_PATH_ENV not in os.environ:
        raise RuntimeError('{} environment variable is not set'.format(ROOT_PROJECT_PATH_ENV))

def getArgs():
    parser = argparse.ArgumentParser(description='Generate aggregated report for checkstyle lint errors.')
    parser.add_argument('-p', dest='project', default=None, help='LE project name to generate the report, default is the root project.')
    parser.add_argument('-v', dest='verbose', action="store_true", help='Log the aggregated report to stdout.')
    return parser.parse_args()


def findAllXmls(proj_dir):
    projectList = []
    for root, dirs, files in os.walk(proj_dir):
        for name in files:
            if name == SPOTBUGS_RESULT_FILE:
                xml_file = "%s/%s" % (root, name)
                numBugs, projectName, patternCntr, codeCntr, categoryCntr, fileCntrs = parseXml(xml_file)
                if numBugs > 0:
                    projectList.append((numBugs, projectName, patternCntr, codeCntr, categoryCntr, fileCntrs))
    projectList.sort(key=lambda x: x[0], reverse=True)
    return projectList

def parseXml(xml_file):
    root = xml.parse(xml_file)
    collection = root.getElementsByTagName('BugCollection')[0]
    project = collection.getElementsByTagName('Project')[0]
    projectName = project.attributes['projectName'].value
    numBugs, patternCntr, codeCntr, categoryCntr, fileCntrs = parseBugInstances(collection)
    return numBugs, projectName, patternCntr, codeCntr, categoryCntr, fileCntrs


def parseBugInstances(collection):
    """
    Go thru all BugInstance nodes, aggregate counts at patttern -> code -> category levels
    for each file and the whole project
    :param collection:
    :return:
    """
    fileCntrs = {}
    patternCntr = Counter()
    codeCntr = Counter()
    categoryCntr = Counter()
    numBugs = len(collection.getElementsByTagName('BugInstance'))
    for bug in collection.getElementsByTagName('BugInstance'):
        srcFile = bug.getElementsByTagName('Class')[0].getElementsByTagName('SourceLine')[0].attributes['sourcepath'].value

        fileCntr = fileCntrs.get(srcFile, {})

        rank = bug.attributes['rank'].value
        confidence = bug.attributes['priority'].value

        pattern = bug.attributes['type'].value + "(%s|%s)" % (confidence, rank)
        fileCntr['pattern'] =  fileCntr.get('pattern', Counter()) + Counter([pattern])
        patternCntr += Counter([pattern])

        code = bug.attributes['abbrev'].value
        fileCntr['code'] =  fileCntr.get('code', Counter()) + Counter([code])
        codeCntr += Counter([code])

        category = bug.attributes['category'].value
        fileCntr['category'] =  fileCntr.get('category', Counter()) + Counter([category])
        categoryCntr += Counter([category])

        fileCntr['bugs'] =  fileCntr.get('bugs', 0) + 1

        fileCntrs[srcFile] = fileCntr

    return numBugs, patternCntr, codeCntr, categoryCntr, fileCntrs


def printCounters(patternCntr, codeCntr, categoryCnter, indent=0):
    msg = ""
    msg += " " * indent + "by category:\n"
    for item in categoryCnter.most_common():
        msg += " " * (indent + 1) + "%s: %d\n" % item
    msg += " " * indent + "by code:\n"
    for item in codeCntr.most_common():
        msg += " " * (indent + 1) + "%s: %d\n" % item
    msg += " " * indent + "by patterns:\n"
    for item in patternCntr.most_common():
        msg += " " * (indent + 1) + "%s: %d\n" % item
    return msg


if __name__ == '__main__':
    main()
