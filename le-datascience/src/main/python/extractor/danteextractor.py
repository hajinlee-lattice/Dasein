import csv
import itertools
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
                analyticAttributes = lead["AnalyticAttributes"]
                
                internal = []
                external = []
                for analyticAttr in analyticAttributes:
                    name = analyticAttr["AttributeName"]
                    predictor = predictors[name]
                    
                    for pe in predictor["Elements"]:
                        value = analyticAttr["AttributeValue"]
                        if value is not None and pe["LowerInclusive"] is None and pe["UpperExclusive"] is None and pe["Values"][0] == value:
                            t = (name, value, pe["Lift"])
                            if columnToTag[name] == "External":
                                external.append(t)
                            else:
                                internal.append(t)
                        lowerInclusive = pe["LowerInclusive"]
                        upperExclusive = pe["UpperExclusive"]
                        if value is not None and (lowerInclusive is not None or upperExclusive is not None):
                            if lowerInclusive is not None and upperExclusive is not None and value >= lowerInclusive and value < upperExclusive:
                                t = (name, value, pe["Lift"])
                            elif lowerInclusive is None and value < upperExclusive:
                                t = (name, value, pe["Lift"])
                            elif upperExclusive is None and value >= lowerInclusive:
                                t = (name, value, pe["Lift"])
                
                e = sorted(external, key = lambda x: x[2], reverse=True)
                i = sorted(internal, key = lambda x: x[2], reverse=True)
                
                if len(e) > 5:
                    e = e[0:4]
                if len(i) > 5:
                    i = i[0:4]
                merged_e = list(itertools.chain.from_iterable(e))
                merged_i = list(itertools.chain.from_iterable(i))
                l = [lead["LeadID"], "%d" % (lead["Probability"] * 100), lead["Percentile"]]
                l.extend(merged_i)
                l.extend(merged_e)
                csvwriter.writerow(l)
                
            


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
    
