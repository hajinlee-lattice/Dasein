#!/usr/bin/python

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
        for root, dirnames, filenames in os.walk(curDir):
            updatePreInstallFile(root, filenames)
            updatePostInstallFile(root, filenames)

def printCurrentDir(dir):
    print LINE_SEPERATOR
    print "Current directory is: " + dir

def updatePreInstallFile(root, filenames):
    for filename in fnmatch.filter(filenames, 'tcpreinstall_tc'):
        print (os.path.join(root, filename))
        oldFile = open(os.path.join(root, filename))
        oldFileContents = oldFile.readlines()
        newFileName = filename + ".tmp"
        newFile = open(os.path.join(root,newFileName), 'w')
        foundAddGroup = False
        for i in range(len(oldFileContents)):
            line = oldFileContents[i]
            print line
            newFile.write(line)
            if (line.startswith('groupadd')):
                newFile.write('groupadd tomcat\n')
                foundAddGroup = True
        if not foundAddGroup:
            newFile.write('groupadd tomcat\n')
        oldFile.close()
        newFile.close()
        os.remove(os.path.join(root, filename))
        shutil.move(os.path.join(root,newFileName), os.path.join(root, filename))

def updatePostInstallFile(root, filenames):
    for filename in fnmatch.filter(filenames, 'postinstall_tc'):
        oldFile = open(os.path.join(root, filename))
        oldFileContents = oldFile.readlines()
        newFileName = filename + ".tmp"
        newFile = open(os.path.join(root,newFileName), 'w')
        foundAddGroup = False
        for i in range(len(oldFileContents)):
            line = oldFileContents[i]
            print line
            newFile.write(line)
            if (line.find('finishing') != -1):
                newFile.write('chown -R root:tomcat $CATALINA_HOME/webapps\n')
                foundAddGroup = True
        if not foundAddGroup:
            newFile.write('chown -R root:tomcat $CATALINA_HOME/webapps\n')
            
        oldFile.close()
        newFile.close()
        os.remove(os.path.join(root, filename))
        shutil.move(os.path.join(root,newFileName), os.path.join(root, filename))
    

if __name__ == "__main__":
    main(sys.argv[1:])


