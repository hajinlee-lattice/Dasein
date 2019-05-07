package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;

public class LookupIdMappingResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    private String orgId = "Org_" + System.currentTimeMillis();
    private String orgName = "Dummy name";
    private CDLExternalSystemType externalSystemType = CDLExternalSystemType.CRM;
    private String configId = null;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void getLookupIdsMapping() {
        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(mainCustomerSpace,
                null, null, true);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertEquals(lookupIdConfigs.keySet().size(), 1);
        Assert.assertTrue(lookupIdConfigs.containsKey(CDLExternalSystemType.FILE_SYSTEM.name()));
    }

    @Test(groups = "deployment-app")
    public void registerExternalSystem() {
        LookupIdMap lookupIdsMap = new LookupIdMap();
        lookupIdsMap.setOrgId(orgId);
        lookupIdsMap.setOrgName(orgName);
        lookupIdsMap.setExternalSystemType(externalSystemType);

        LookupIdMap resultLookupIdMap = lookupIdMappingProxy.registerExternalSystem(mainCustomerSpace, lookupIdsMap);
        Assert.assertNotNull(resultLookupIdMap);
        Assert.assertEquals(resultLookupIdMap.getOrgId(), lookupIdsMap.getOrgId());
        Assert.assertEquals(resultLookupIdMap.getOrgName(), lookupIdsMap.getOrgName());
        Assert.assertEquals(resultLookupIdMap.getExternalSystemType(), lookupIdsMap.getExternalSystemType());
        Assert.assertNotNull(resultLookupIdMap.getId());
        Assert.assertNotNull(resultLookupIdMap.getIsRegistered());
        Assert.assertEquals(resultLookupIdMap.getIsRegistered(), Boolean.TRUE);

        configId = resultLookupIdMap.getId();

        confirmNonEmptyLookupConfigs(Boolean.TRUE);
    }

    @Test(groups = "deployment-app")
    public void updateLookupIdMap() {

        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(mainCustomerSpace,
                null, null, true);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() > 0);

        lookupIdConfigs.keySet().forEach(k -> {
            Assert.assertTrue(lookupIdConfigs.get(k).size() > 0);
            lookupIdConfigs.get(k).forEach(c -> {
                Assert.assertNotNull(c);
                Assert.assertNotNull(c.getId());
                Assert.assertNotNull(c.getOrgId());
                Assert.assertNotNull(c.getOrgName());
                Assert.assertNotNull(c.getIsRegistered());
                Assert.assertEquals(c.getIsRegistered(), Boolean.TRUE);

                String accountId = "Acc_" + System.currentTimeMillis();
                String description = "Some desc";
                c.setAccountId(accountId);
                c.setDescription(description);
                lookupIdMappingProxy.updateLookupIdMap(mainCustomerSpace, c.getId(), c);
                LookupIdMap lookupIdMap = lookupIdMappingProxy.getLookupIdMap(mainCustomerSpace, c.getId());
                Assert.assertNotNull(lookupIdMap);
                Assert.assertEquals(lookupIdMap.getExternalSystemType(), c.getExternalSystemType());
                Assert.assertEquals(lookupIdMap.getId(), c.getId());
                Assert.assertEquals(lookupIdMap.getOrgId(), c.getOrgId());
                Assert.assertEquals(lookupIdMap.getOrgName(), c.getOrgName());
                Assert.assertEquals(lookupIdMap.getId(), c.getId());
                Assert.assertEquals(lookupIdMap.getAccountId(), accountId);
                Assert.assertEquals(lookupIdMap.getDescription(), description);
                Assert.assertNotNull(lookupIdMap.getIsRegistered());
                Assert.assertEquals(lookupIdMap.getIsRegistered(), c.getIsRegistered());

            });
        });

    }

    @Test(groups = "deployment-app")
    public void testDeregisterExternalSystem() {
        LookupIdMap configBeforeDeregister = lookupIdMappingProxy.getLookupIdMap(mainCustomerSpace, configId);
        Assert.assertEquals(configBeforeDeregister.getExternalSystemType(), externalSystemType);
        Assert.assertEquals(configBeforeDeregister.getId(), configId);
        Assert.assertEquals(configBeforeDeregister.getOrgId(), orgId);
        Assert.assertEquals(configBeforeDeregister.getOrgName(), orgName);
        Assert.assertEquals(configBeforeDeregister.getIsRegistered(), Boolean.TRUE);

        LookupIdMap lookupIdsMap = new LookupIdMap();
        lookupIdsMap.setOrgId(orgId);
        lookupIdsMap.setOrgName(orgName);
        lookupIdsMap.setExternalSystemType(externalSystemType);

        lookupIdMappingProxy.deregisterExternalSystem(mainCustomerSpace, lookupIdsMap);

        LookupIdMap configAfterDeregister = lookupIdMappingProxy.getLookupIdMap(mainCustomerSpace, configId);
        Assert.assertEquals(configAfterDeregister.getExternalSystemType(), externalSystemType);
        Assert.assertEquals(configAfterDeregister.getId(), configId);
        Assert.assertEquals(configAfterDeregister.getOrgId(), orgId);
        Assert.assertEquals(configAfterDeregister.getOrgName(), orgName);
        Assert.assertEquals(configAfterDeregister.getIsRegistered(), Boolean.FALSE);

        confirmNonEmptyLookupConfigs(Boolean.FALSE);

        LookupIdMap resultLookupIdMap = lookupIdMappingProxy.registerExternalSystem(mainCustomerSpace, lookupIdsMap);
        Assert.assertNotNull(resultLookupIdMap);
        Assert.assertEquals(resultLookupIdMap.getOrgId(), lookupIdsMap.getOrgId());
        Assert.assertEquals(resultLookupIdMap.getOrgName(), lookupIdsMap.getOrgName());
        Assert.assertEquals(resultLookupIdMap.getExternalSystemType(), lookupIdsMap.getExternalSystemType());
        Assert.assertNotNull(resultLookupIdMap.getId());
        Assert.assertNotNull(resultLookupIdMap.getIsRegistered());
        Assert.assertEquals(resultLookupIdMap.getIsRegistered(), Boolean.TRUE);

        LookupIdMap configAfterAnotherRegister = lookupIdMappingProxy.getLookupIdMap(mainCustomerSpace, configId);
        Assert.assertEquals(configAfterAnotherRegister.getExternalSystemType(), externalSystemType);
        Assert.assertEquals(configAfterAnotherRegister.getId(), configId);
        Assert.assertEquals(configAfterAnotherRegister.getOrgId(), orgId);
        Assert.assertEquals(configAfterAnotherRegister.getOrgName(), orgName);
        Assert.assertEquals(configAfterAnotherRegister.getIsRegistered(), Boolean.TRUE);

        confirmNonEmptyLookupConfigs(Boolean.TRUE);
    }

    // TODO - anoop - enable it
    @Test(groups = "deployment-app", enabled = false)
    public void getAllLookupIds() {
        Map<String, List<CDLExternalSystemMapping>> allLookupIds = lookupIdMappingProxy
                .getAllLookupIds(mainCustomerSpace, null);
        Assert.assertNotNull(allLookupIds);
        Assert.assertTrue(allLookupIds.keySet().size() > 0);

        allLookupIds.keySet().forEach(k -> {
            CDLExternalSystemType externalSystemType = CDLExternalSystemType.valueOf(k);
            Assert.assertNotNull(externalSystemType);
            Assert.assertTrue(allLookupIds.get(k).size() > 0);
            allLookupIds.get(k).forEach(c -> {
                Assert.assertNotNull(c);
                Assert.assertNotNull(c.getDisplayName());
                Assert.assertNotNull(c.getFieldName());
                Assert.assertNotNull(c.getFieldType());
            });
        });

    }

    @Test(groups = "deployment-app")
    public void getAllCDLExternalSystemType() {
        List<CDLExternalSystemType> allCDLExternalSystemType = lookupIdMappingProxy
                .getAllCDLExternalSystemType(mainCustomerSpace);
        Assert.assertNotNull(allCDLExternalSystemType);
        Assert.assertTrue(CollectionUtils.isNotEmpty(allCDLExternalSystemType));
    }

    private void confirmNonEmptyLookupConfigs(Boolean isMarkedRegistered) {
        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(mainCustomerSpace,
                null, null, true);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() > 0);

        lookupIdConfigs.keySet().stream().filter(k -> !k.equals(CDLExternalSystemType.FILE_SYSTEM.name()))
                .forEach(k -> {
                    CDLExternalSystemType externalSystemType = CDLExternalSystemType.valueOf(k);
                    Assert.assertTrue(lookupIdConfigs.get(k).size() > 0);
                    lookupIdConfigs.get(k).forEach(c -> {
                        Assert.assertNotNull(c);
                        Assert.assertEquals(c.getExternalSystemType(), externalSystemType);
                        Assert.assertNotNull(c.getId());
                        Assert.assertNotNull(c.getOrgId());
                        Assert.assertNotNull(c.getOrgName());

                        LookupIdMap lookupIdMap = lookupIdMappingProxy.getLookupIdMap(mainCustomerSpace, c.getId());
                        Assert.assertNotNull(lookupIdMap);
                        Assert.assertEquals(lookupIdMap.getExternalSystemType(), externalSystemType);
                        Assert.assertNotNull(lookupIdMap.getId());
                        Assert.assertNotNull(lookupIdMap.getOrgId());
                        Assert.assertNotNull(lookupIdMap.getOrgName());
                        Assert.assertEquals(lookupIdMap.getId(), c.getId());
                        Assert.assertNotNull(lookupIdMap.getIsRegistered());
                        Assert.assertEquals(lookupIdMap.getIsRegistered(), isMarkedRegistered);
                    });
                });
    }
}
