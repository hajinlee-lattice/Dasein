from unittest import TestCase
from ambariextractor.clusterinfoextractor import ClusterInfoExtractor

class ClusterInfoExtractorTest(TestCase):
    
    def testExtract(self):
        extractor = ClusterInfoExtractor()
        extractor.extract("bodcdevvhort147.lattice.local", "8086", "admin", "admin", "ledp_dev_cluster")
        