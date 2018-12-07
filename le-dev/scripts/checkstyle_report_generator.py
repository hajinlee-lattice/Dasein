from __future__ import print_function

import argparse
import operator
import os
import re
import xml.dom.minidom as xml

ROOT_PROJECT_PATH_ENV = 'WSHOME'
TARGET_DIR = 'target'
CHECKSTYLE_RESULT_FILE = 'checkstyle-result.xml'
AGGREGATED_REPORT_FILE = 'aggregated_checkstyle_report.txt'

def main():
    check()
    args = getArgs()
    srcRootPath, checkStyleFilePath, outputFilePath = getPaths(args)
    checkPathExists(checkStyleFilePath)
    report = generateReport(srcRootPath, checkStyleFilePath)
    exportReport(args.verbose, outputFilePath, report)


def check():
    # check required environment variables
    if ROOT_PROJECT_PATH_ENV not in os.environ:
        raise RuntimeError('{} environment variable is not set'.format(ROOT_PROJECT_PATH_ENV))


def checkPathExists(resultFilePath):
    # checkstyle result file must be already generated
    if not os.path.exists(resultFilePath):
        raise RuntimeError('Checkstyle result file does not exist, path={}'.format(resultFilePath))


def getPaths(args):
    """Extract command line arguments and return as a map.

    Args:
        args (Dict(str: any)): Dictionary that maps command line argument name to value
    Returns:
        (string, string, string): path to source root, raw checkstyle result file and report output file
    """
    # return (PROJECT_ROOT, INPUT_FILE, OUTPUT_FILE)
    rootProjPath = os.environ[ROOT_PROJECT_PATH_ENV]
    srcRootPath = os.path.join(rootProjPath, 'le-$PROJECT', 'src/main/java') # with placeholder since there are multiple projects
    projPath = rootProjPath
    if args.project:
        projPath = os.path.join(projPath, 'le-{}'.format(args.project))
        srcRootPath = os.path.join(projPath, 'src/main/java')
    checkStyleFilePath = os.path.join(projPath, TARGET_DIR, CHECKSTYLE_RESULT_FILE)
    outputFilePath = os.path.join(projPath, TARGET_DIR, AGGREGATED_REPORT_FILE)
    return srcRootPath, checkStyleFilePath, outputFilePath


def getArgs():
    """Extract command line arguments and return as a map.

    Returns:
        Dict(str: any): Dictionary that maps command line argument name to value
    """
    parser = argparse.ArgumentParser(description='Generate aggregated report for checkstyle lint errors.')
    parser.add_argument('--proj', dest='project', default=None, help='LE project name to generate the report, default is the root project.')
    parser.add_argument('-v', dest='verbose', action="store_true", help='Log the aggregated report to stdout.')
    return parser.parse_args()


def generateReport(srcRootPath, checkStyleResultFilePath):
    """Generate aggregated (on project and file level) report from raw checkstyle result.

    Args:
        srcRootPath (str): path to the root source files, only for display purpose
        checkStyleResultFilePath (str): path to raw checkstyle result
    Returns:
        List[str]: list of report strings
    """
    report = []
    # read checkstyle result xml file and extract all files that contain lint errors
    # Result file format:
    # <checkstyle>
    #   <file name="<FILE_NAME>">
    #     <error></error>
    #     <error></error>
    #     ...
    #   </file>
    #   ...
    # </checkstyle>
    root = xml.parse(checkStyleResultFilePath)
    checkStyleRoot = root.getElementsByTagName('checkstyle')[0] # should only have one checkstyle tag
    fileEles = checkStyleRoot.getElementsByTagName('file')

    # projectErrCntMap: key=project name, value=total lint errors in the project
    # fileErrCntMap: key=project, value=List(lint error info in one file)
    projectErrCntMap, fileErrCntMap = {}, {}
    # files that contain check style errors
    for ele in fileEles:
        fileName = ele.attributes['name'].value
        errorEles = ele.getElementsByTagName('error') # error elements
        errCnt = len(errorEles) # number of error in this file

        # retrieve information from file name, format: le-<PROJECT_NAME>/src/(main|test)/java/<SRC_FILE>
        # 1. use non-greedy mode *? to trim anything before le-${PROJECT}/src/main/java
        # 2. use non-greedy mode +? in the first regex group to retrieve project name
        # 3. retrieve package name, class name from the second group (path after source root)
        match = re.search(r'.*?le-([a-zA-Z0-9\-/]+?)/src/(main|test)/java/(.*\.java)', fileName)
        if match:
            project = match.group(1)
            srcFilePath = match.group(2)
            fullClassName = srcFilePath.replace('/', '.').replace('.java', '')
            className = fullClassName[fullClassName.rfind('.') + 1:] # get all chars after the LAST .

            if project not in projectErrCntMap:
                projectErrCntMap[project] = 0
                fileErrCntMap[project] = []

            projectErrCntMap[project] += errCnt # increase error count in project
            fileErrCntMap[project].append({
                'fullFilePath': fileName,
                'srcFilePath': srcFilePath,
                'fullClassName': fullClassName,
                'className': className,
                'numErrors': errCnt
            }) # append error information for one file to the project list
        else:
            raise RuntimeError('Invalid source filename {}'.format(fileName))

    # sort by total error count in project DESC
    # [ project name, total error count ]
    projectErrorCnts = sorted(projectErrCntMap.items(), key=operator.itemgetter(1), reverse=True)

    report.append('Checkstyle result file: {}'.format(checkStyleResultFilePath))
    report.append('Root Dir: {}'.format(srcRootPath))
    nErrors = sum(n for _, n in projectErrorCnts)
    nFiles = sum(len(fileErrCntMap[p]) for p, _ in projectErrorCnts)
    report.append('%d errors in %d files in total.' % (nErrors, nFiles))
    if nErrors == 0:
        print('Great! There is no errors.')
    else:
        print('%d errors in %d files in total.' % (nErrors, nFiles))

    for t in projectErrorCnts:
        project, totalErrs = t
        report.append('\n{} total errors in project [{}]:'.format(totalErrs, project))

        # sort by number of error in the file DESC
        files = sorted(fileErrCntMap[project], key=operator.itemgetter('numErrors'), reverse=True)
        for f in files:
            # class name is aligned with length 65, error count is aligned with length 3
            # NOTE add a space when error is singular to keep alignment
            # NOTE if we have 1000 errors in one file or the class name is longer than 65 characters, we will
            #      have formatting problem. should not happen.
            nErrs = f['numErrors']
            report.append('- {:3d} error{} in class {:65s} File={}'.format(
                nErrs, 's' if nErrs > 1 else ' ', f['className'], f['fullFilePath']))
    return report


def exportReport(logReportToConsole, outputFilePath, report):
    """Export the generated report.

    Args:
        logReportToConsole (bool): flag to export the report to console
        outputFilePath (str): output file path to export the report, file at the destination will be overwritten
        report (List[str]): List of report strings
    """
    with open(outputFilePath, 'w') as f:
        f.writelines("%s\n" % l for l in report)
    print("report is at %s" % outputFilePath)
    if logReportToConsole:
        for l in report:
            print(l)


if __name__ == '__main__':
    main()
