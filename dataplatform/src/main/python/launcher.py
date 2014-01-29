import sys
from leframework import argumentparser


def stripPath(fileName):
    #return fileName
    return fileName[fileName.rfind('/')+1 : fileName.__len__()]

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- training data file
    sys.argv[2] -- test data file
    sys.argv[3] -- schema json file
    sys.argv[4] -- user python script
    """

    parser = argumentparser.createParser(stripPath(sys.argv[3]))
    training = parser.createList(stripPath(sys.argv[1]))
    test = parser.createList(stripPath(sys.argv[2]))
    script = stripPath(sys.argv[4])
    execfile(script)
    globals()['train'](training, test, parser.getSchema())
    
