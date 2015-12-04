'''
Created on Dec 1, 2015

@author: smeng
'''

from kazoo.client import KazooClient
import json
import logging


class ZookeeperUtility():

    logger = logging.getLogger('zkutility')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    def __init__(self, pod='QA', address='bodcdevvhdp106.dev.lattice.local:2181,bodcdevvhdp109.dev.lattice.local:2181,bodcdevvhdp113.dev.lattice.local:2181'):
        self.zk = KazooClient(address)
        self.pod = pod
        self.startZookeeper()


    def startZookeeper(self):
        self.zk.start()

    def stopZookeeper(self):
        self.zk.stop()

    def cleanUpOrgId(self, orgId):
        self.logger.info('--------Cleaning up tenants orgIds in POD ' + self.pod + ' where OrgId=' + orgId)
        contracts_path = '/Pods/' + self.pod + '/Contracts'
        contracts = self.zk.get_children(contracts_path)
        for contract in contracts:
            path = self._getPropertyPath(contract)
            if self.zk.exists(path):
                propertiesJson = json.loads(self.zk.get(path)[0])
                if propertiesJson['sfdcOrgId'] == orgId:
                    self._setOrgId(contract, path, 'Dummy')

    def _getPropertyPath(self, tenant):
        return '/Pods/' + self.pod + '/Contracts' + '/' + tenant + '/Tenants/' + tenant + '/Spaces/Production/properties.json'

    def _setOrgId(self, tenant, path, orgId):
        self.logger.info('----updating OrgId for tenant: ' + tenant)
        self.logger.info('property before update: ' + self.zk.get(path)[0])
        propertiesJson = json.loads(self.zk.get(path)[0])
        propertiesJson['sfdcOrgId'] = orgId
        self.zk.set(path, json.dumps(propertiesJson))
        self.logger.info('property after update: ' + self.zk.get(path)[0])

    def updateOrgId(self, tenant, orgId):
        path = self._getPropertyPath(tenant)
        if self.zk.exists(path):
            self._setOrgId(tenant, path, orgId)

    def getProperties(self, tenant):
        path = self._getPropertyPath(tenant)
        properties = ''
        if self.zk.exists(path):
            properties = self.zk.get(path)[0]
            # self.logger.info('properties: ' + properties)
        return properties

    def getOrgId(self, tenant):
        path = self._getPropertyPath(tenant)
        orgId = ''
        if self.zk.exists(path):
            propertiesJson = json.loads(self.zk.get(path)[0])
            orgId = propertiesJson['sfdcOrgId']
            # self.logger.info('OrgId: ' + orgId)
        return orgId

    def createFakeTenant(self, tenant):
        self.zk.ensure_path(self._getPropertyPath(tenant))
        self.logger.info('created fake tenant: ' + tenant)

    def deleteTenant(self, tenant):
        path = '/Pods/' + self.pod + '/Contracts' + '/' + tenant
        self.zk.delete(path, recursive=True)
        self.logger.info('deleted tenant: ' + tenant)

    def listTenants(self):
        contracts_path = '/Pods/' + self.pod + '/Contracts'
        contracts = self.zk.get_children(contracts_path)
        self.logger.info('----------- All TENANTS UNDER POD ' + self.pod + ' -----------')
        for contract in contracts:
            self.logger.info(contract)

    def displayNode(self, path):
        if self.zk.exists(path):
            self.logger.info(self.zk.get(path)[0])
        else:
            self.logger.info('Node not exists')

    def exists(self, tenant):
        path = '/Pods/' + self.pod + '/Contracts' + '/' + tenant
        ex = self.zk.exists(path)
        return (None != ex)

    def ls(self, path):
        if self.zk.exists(path):
            self.logger.info([str(x) for x in self.zk.get_children(path)])
        else:
            self.logger.info('path not exists')

if __name__ == '__main__':
    pass
