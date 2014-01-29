import json
import csv

class ArgumentParser(object):
    def __init__(self, schema):
        jsonData = open(schema).read()
        self.schema = json.loads(jsonData)
        self.fields = self.schema["schema"]["fields"]
        
    def convertType(self, cell, index):
        print(self.fields[index])
        fieldType = self.fields[index]["type"][0]
        
        if fieldType == "int":
            return int(cell)
        elif fieldType == "float":
            return float(cell)
        return cell

    def createList(self, dataFileName):
        csvfile = open(dataFileName, 'Ur')
        tmp = []
        for row in csv.reader(csvfile, delimiter=','):
            rowlist = []
            for i in range(len(row)):
                rowlist.append(self.convertType(row[i], i))
            tmp.append(rowlist)
        return tmp
    
    def getSchema(self):
        return self.schema

def createParser(schema):
    return ArgumentParser(schema)

