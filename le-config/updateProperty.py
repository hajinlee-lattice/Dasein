# This script is used to update property file in specified environment
import glob
import os
import shutil

PROPERTY_DIR = "/conf/env/"
PROPERTY_FILE_SUFFIX = "*.properties"
DICTIONARY_FILE = "le-config/profile.properties"
LINE_SEPERATOR = "\n=============================\n"
ENVIRONMENTS=('qaclustera', 'qaclusterb', 'prodclustera', 'prodclusterb')

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    topLevelDir = os.getcwd()
    print "Change to top level directory: " + topLevelDir

    dictionaryFileName = DICTIONARY_FILE
    dictionaryFile = open(dictionaryFileName)
    dictionaryContents = dictionaryFile.readlines()
    dictionary = loadContents(dictionaryContents)
    dictionaryFile.close()

    for dirName, subdirList, fileList in os.walk('.'):
        for environment in ENVIRONMENTS:
            end_with = PROPERTY_DIR + environment
            if dirName[-len(end_with):] == end_with:
                updatePropertyFiles(environment, dirName, dictionary)


def printCurrentDir(dir):
    print LINE_SEPERATOR
    print "Current directory is: " + dir

def updatePropertyFiles(environment, dir, dictionary):
    qaPropertyFiles = glob.glob(dir + '/' + PROPERTY_FILE_SUFFIX)

    #open specific property file and compare the contents of them
    for pFile in qaPropertyFiles:
        qaPropertyFileName = pFile
        qaNewPropertyFileName = pFile + ".tmp"
        qaPropertyFile = open(pFile)
        qaPropertyConents = qaPropertyFile.readlines()
        qaNewPropertyFile = open(qaNewPropertyFileName, 'w')
        updateContents(environment, pFile.split('/')[-1], qaPropertyConents, dictionary, qaNewPropertyFile)
        qaPropertyFile.close()
        qaNewPropertyFile.close()
        os.remove(qaPropertyFileName)
        shutil.move(qaNewPropertyFileName, qaPropertyFileName)


def updateContents(environment, filename, conents, dictionary, file):
    for i in range(len(conents)):
        line = conents[i]
        if (not line.startswith( '#' )) and (not line.isspace() ):
            pair = line.split('=', 1)
            if ('${INSIDE_TOMCAT}' in pair[1]):
                line = pair[0] + '=' + dictionary[pair[0]] \
                    .replace('${INSIDE_TOMCAT}', dictionary['.%s.inside.tomcat' % environment])
            if ('${OUTSIDE_TOMCAT}' in pair[1]):
                line = pair[0] + '=' + dictionary[pair[0]] \
                    .replace('${OUTSIDE_TOMCAT}', dictionary['.%s.outside.tomcat' % environment])
                print environment + ' : ' + filename + " : " + line.replace('\n', '')
        file.write(line)

def loadContents(conents):
    dictionary = {}
    for i in range(len(conents)):
        line = conents[i]
        if (not line.startswith( '#' )) and (not line.isspace() ):
            pair = line.split('=', 1)
            if len(pair) == 2:
                dictionary[pair[0]] = pair[1]
            else:
                dictionary[pair[0]] = ''

            if '.qacluster' in pair[0] or '.prodcluster' in pair[0]:
                dictionary[pair[0]] = dictionary[pair[0]].replace('\n', '')
                print '%s=%s' % (pair[0], dictionary[pair[0]])
    print ''

    return dictionary

if __name__ == "__main__":
    main()


