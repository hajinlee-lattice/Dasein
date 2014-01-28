import csv
import sys
import json

class Launcher(object):
    def __init__(self):
        pass 
        
    def convertToList(self, dataFileName):
        with open(dataFileName, 'Ur') as f:
            data = list(tuple(rec) for rec in csv.reader(f, delimiter=','))
        return data
   
def stripPath(fileName):
    return fileName
    #return fileName[fileName.rfind('/')+1 : fileName.__len__()]

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- training data file
    sys.argv[2] -- test data file
    sys.argv[3] -- schema json file
    sys.argv[4] -- user python script
    """

    l = Launcher()
    jsonData = open(stripPath(sys.argv[3])).read()
    schema = json.loads(jsonData)
    training = l.convertToList(stripPath(sys.argv[1]), schema)
    test = l.convertToList(stripPath(sys.argv[2]), schema)
    script = stripPath(sys.argv[4])
    execfile(script)
    globals()['train'](training, test, schema)
    
