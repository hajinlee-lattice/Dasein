import unittest
import leadlevelpredictor.querybuilder as qbldr

class QueryBuilderTest(unittest.TestCase):
    
    def testQuery(self):
        self.qb = qbldr.QueryBuilder("sqlserverdatasource", "DataLoader_Dep", "L@ttice1", "EventTableTestingDB")
        rset = self.qb.executeQuery("select * from dbo.Nutanix_EventTable")
        
        for row in rset:
            print(row)

        