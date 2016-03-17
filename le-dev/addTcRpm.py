# This script is used to update property file in specified environment
import sys
import os
import glob
import json
import shutil
import fnmatch
import re
import shutil
import errno

HDFS_SUFFIX = "*-hdfs.xml"
RPM_SUFFIX = "*-rpm.xml"
TC_SUFFIX = "*-tc-*.xml"

POSTINSTALL = "postinstall"
PREINSTALL = "preinstall"
PREREMOVE = "preremove"

LINE_SEPERATOR = "\n=============================\n"

def main(argv):
    print "Current directory should be in le-dev: " + os.getcwd()

    projects = argv
    print projects
    os.chdir(os.getcwd()+"/..")
    topLevel = os.getcwd()
    print "Change to top level directory: " + topLevel

    #allDirsAndFiles = os.listdir(os.getcwd())

    for dir in projects:
        curDir = topLevel + "/" + dir
        # change directory to each project, and see if they have rpm files and scripts
        if rpmFileExists(curDir) and tcRpmFileNotExists(curDir):
            updateFiles(curDir)

def rpmFileExists(curDir):
    allDirsAndFiles = os.listdir(curDir)
    for dir in allDirsAndFiles:
        if not os.path.isdir(dir):
            if fnmatch.fnmatch(dir, HDFS_SUFFIX) or fnmatch.fnmatch(dir, RPM_SUFFIX):
                return True
    return False

def tcRpmFileNotExists(curDir):
    allDirsAndFiles = os.listdir(curDir)
    for dir in allDirsAndFiles:
        if not os.path.isdir(dir):
            if fnmatch.fnmatch(dir, TC_SUFFIX):
                return False
    return True

def printCurrentDir(dir):
    print LINE_SEPERATOR
    print "Current directory is: " + dir

def updateFiles(dir):
    allDirsAndFiles = os.listdir(dir)
    for file in allDirsAndFiles:
        if not os.path.isdir(file):
            if fnmatch.fnmatch(file, HDFS_SUFFIX):
                print (os.path.join(dir, file))
                index = str(file).rfind('-')
                newName = str(file)[0:index] + '-'+ 'tc' + str(file[index:])
                shutil.copyfile(os.path.join(dir, file), os.path.join(dir, newName))
                
    for root, dirnames, filenames in os.walk(dir):
        copyfile(root, filenames, 'postinstall')
        copyfile(root, filenames, 'preinstall')
        copyfile(root, filenames, 'preremove')

def copyfile(root, filenames, pattern):
    for filename in fnmatch.filter(filenames, pattern):
        print (os.path.join(root, filename))
        newFile = 'tc'+pattern
        silentremove(os.path.join(root, newFile))
        shutil.copyfile(os.path.join(root, filename), os.path.join(root, newFile))
        
def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

if __name__ == "__main__":
    main(sys.argv[1:])


