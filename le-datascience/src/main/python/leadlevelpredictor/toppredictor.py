import json
import sys

class TopPredictor(object):
    
    def __init__(self, filename):
        self.filename = filename 
        self.jsonObject = json.loads(open(filename).read())
        
    
    def getTopPredictors(self):
        predictors = self.jsonObject['Summary']['Predictors']
        for predictor in predictors:
            print("%s : %f" % (predictor['Name'], predictor['UncertaintyCoefficient']))
            

if __name__ == "__main__":
    tp = TopPredictor(sys.argv[1])
    tp.getTopPredictors()
