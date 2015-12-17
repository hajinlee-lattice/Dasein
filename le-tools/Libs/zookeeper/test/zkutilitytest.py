'''
Created on Dec 1, 2015

@author: smeng
'''
import unittest
from Libs.zookeeper.zkutility import ZookeeperUtility


class Test(unittest.TestCase):


    def setUp(self):
        self.zkutil = ZookeeperUtility(pod='SimonTest')
        self.zkutil.updateOrgId('Test1', 'before')
        self.zkutil.updateOrgId('Test2', 'before')
        self.zkutil.updateOrgId('Test4', 'NoChange')

    def tearDown(self):
        self.zkutil.stopZookeeper()

    def testCleanUpOrgId(self):
        self.zkutil.cleanUpOrgId("before")
        assert 'Dummy' == self.zkutil.getOrgId('Test1')
        assert 'Dummy' == self.zkutil.getOrgId('Test2')
        assert 'NoChange' == self.zkutil.getOrgId('Test4')


    def testGetOrgId(self):
        assert 'before' == self.zkutil.getOrgId('Test1')
        assert 'before' == self.zkutil.getOrgId('Test2')
        assert 'NoChange' == self.zkutil.getOrgId('Test4')


    def testUpdateOrgId(self):
        self.zkutil.updateOrgId('Test1', 'after')
        assert 'after' == self.zkutil.getOrgId('Test1')
        assert 'before' == self.zkutil.getOrgId('Test2')
        assert 'NoChange' == self.zkutil.getOrgId('Test4')

    def testDeleteTenant(self):
        self.zkutil.createFakeTenant('FakeTenant')
        assert self.zkutil.exists('FakeTenant')
        self.zkutil.deleteTenant('FakeTenant')
        assert not self.zkutil.exists('FakeTenant')

if __name__ == "__main__":
    unittest.main()
