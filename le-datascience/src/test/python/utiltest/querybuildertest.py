import unittest
import util.querybuilder as qbldr

class QueryBuilderTest(unittest.TestCase):
    
    def testQuery(self):
        self.qb = qbldr.QueryBuilder("tynamods", "root", "welcome", "DanteDB")
        rset = self.qb.executeQuery("SELECT * FROM dbo.FrontEndCombinedModelSummaryCache")
        
        for row in rset:
            print(row)

        