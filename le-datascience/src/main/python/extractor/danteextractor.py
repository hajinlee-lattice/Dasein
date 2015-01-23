import csv
import json
from optparse import OptionParser

from util.querybuilder import QueryBuilder


class DanteExtractor(object):
    
    def __init__(self): pass
        
    def extractData(self, options):
        qb1 = QueryBuilder(options["dsn"], options["user"], options["passwd"], options["db"])
        qb2 = QueryBuilder(options["dsn"], options["user"], options["passwd"], options["db"])
        
        summaryrows = qb1.executeQuery("SELECT Value FROM dbo.FrontEndCombinedModelSummaryCache WHERE Customer_ID = '%s'" % options["tenant"])
        leadrows = qb2.executeQuery("SELECT Value FROM dbo.LeadCache WHERE Customer_ID = '%s'" % options["tenant"])
        
        predictors = {}
        columnToTag = {}
        
        for summaryrow in summaryrows.fetchall():
            summary = json.loads(summaryrow[0])
            attributeMetadata = summary["AttributeMetadata"]
            for attribute in attributeMetadata:
                tags = attribute["Tags"]
                for tag in tags:
                    if tag == "Internal" or tag == "External":
                        columnToTag[attribute["ColumnName"]] = tag
            for predictor in summary["Summary"]["Predictors"]:
                predictors[predictor["Name"]] = predictor
        
        with open(options["outfile"], "w") as fw:
            csvwriter = csv.writer(fw, delimiter=",")
            for row in leadrows.fetchall():
                lead = json.loads(row[0])
                if lead["ModelID"] != options["modelname"]:
                    continue
                csvwriter.writerow([lead["LeadID"], lead["Probability"], lead["Percentile"]])
                
            


if __name__ == "__main__":
    de = DanteExtractor()
    parser = OptionParser()
    parser.add_option("-o", "--outfile", dest="outfile")
    parser.add_option("-s", "--dsn", dest="dsn")
    parser.add_option("-d", "--db", dest="db")
    parser.add_option("-u", "--user", dest="user")
    parser.add_option("-p", "--passwd", dest="passwd")
    parser.add_option("-t", "--tenant", dest="tenant")
    parser.add_option("-m", "--model", dest="modelname")
    (options, _) = parser.parse_args()
    de.extractData(options.__dict__)
    
