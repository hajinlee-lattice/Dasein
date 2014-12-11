import csv
import json
import logging
import sys

import leadlevelpredictor.querybuilder as qbldr


logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='data_profile')

class SchemaGenerator(object):
    
    def __init__(self, filename, dsn, username, password, database):
        self.filename = filename
        self.qb = qbldr.QueryBuilder(dsn, username, password, database)
        self.jsonFile = open(filename).read()
        self.jsonObject = json.loads(self.jsonFile)
        
    def getAverageLift(self, eventColumnName, tableName):
        numRows = self.qb.executeQuery("SELECT COUNT(1) FROM dbo.%s" % (tableName))
        rowCount = numRows.fetchall()[0][0]
        numRowsWithEvent = self.qb.executeQuery("SELECT COUNT(1) FROM dbo.%s WHERE %s = 1" % (tableName, eventColumnName))
        rowWithEventCount = numRowsWithEvent.fetchall()[0][0]
        return float(rowWithEventCount)/float(rowCount)

    def getLift(self, eventColumnName, tableName, whereClause):
        numRows = self.qb.executeQuery("SELECT COUNT(1) FROM dbo.%s WHERE %s" % (tableName, whereClause))
        rowCount = numRows.fetchall()[0][0]
        numRowsWithEvent = self.qb.executeQuery("SELECT COUNT(1) FROM dbo.%s WHERE %s = 1 AND %s" % (tableName, eventColumnName, whereClause))
        rowWithEventCount = numRowsWithEvent.fetchall()[0][0]
        if rowCount == 0:
            logger.info("Issue with where clause: %s" % whereClause)
            return 0.0
        return float(rowWithEventCount)/float(rowCount)

    def generate(self, eventColumnName, tableName):
        avgLift = self.getAverageLift(eventColumnName, tableName)
        
        columnMetadataList = self.jsonObject["InputColumnMetadata"]
        
        summaryElementList = self.jsonObject["Summary"]
        
        columnMetadata = {}
    
        for columnMetadataElement in columnMetadataList:
            columnMetadata[columnMetadataElement['Description']] = columnMetadataElement
        
        predictorList = summaryElementList["Predictors"]
        logger.info("Number of input columns: %d" % len(predictorList))
        metadata = []
        for predictor in predictorList:
            name = predictor["Name"]
            
            interpretation = 1
            colname = ""
            if name not in columnMetadata:
                logger.info("Column \"" + name + "\" not in metadata! Using interpretation = 1")
                continue
            else:
                interpretation = columnMetadata[name]["Interpretation"]
                colname = columnMetadata[name]["Name"]
                
            elements = predictor["Elements"]
            
            for el in elements:
                row = [None]*7
                
                if el["Count"] == 0:
                    continue
                
                row[0] = name
                
                whereClause = ""
                if interpretation == 1:
                    row[1] = el["Value"]
                    row[2] = "STR"
                    
                    if row[1] == "null":
                        whereClause = "%s IS NULL"%(colname)
                    else:
                        whereClause = "%s = '%s'"%(colname, row[1])
                else:
                    row[2] = "BND"
                    row[3] = el["UpperExclusive"]
                    row[4] = el["LowerInclusive"]
                    if row[4] == None:
                        row[4] = 0
                    
                    if row[3] != None:
                        whereClause = "%s >= %s AND %s < %s"%(colname, row[4], colname, row[3])
                    else:
                        whereClause = "%s >= %s"%(colname, row[4])

                
                lift = self.getLift(eventColumnName, tableName, whereClause)
                relativeLift = lift/avgLift
                row[5] = relativeLift
                row[6] = columnMetadata[name]["Description"]
                
                metadata.append(row)
            
            ofile = open(self.filename + '.csv', 'wb')
            writer = csv.writer(ofile)
            
            for m in metadata:
                writer.writerow(m)

if __name__ == "__main__":
    schemagen = SchemaGenerator(sys.argv[1])
    schemagen.generate()
