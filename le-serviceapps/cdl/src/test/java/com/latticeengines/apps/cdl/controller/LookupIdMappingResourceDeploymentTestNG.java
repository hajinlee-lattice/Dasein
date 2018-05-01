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

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment")
    public void getLookupIdsMapping() {
        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(mainCustomerSpace,
                null);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() == 0);
    }

    @Test(groups = "deployment")
    public void registerExternalSystem() {
        LookupIdMap lookupIdsMap = new LookupIdMap();
        lookupIdsMap.setOrgId("Org_" + System.currentTimeMillis());
        lookupIdsMap.setOrgName("Dummy name");
        lookupIdsMap.setExternalSystemType(CDLExternalSystemType.CRM);

        LookupIdMap resultLookupIdMap = lookupIdMappingProxy.registerExternalSystem(mainCustomerSpace, lookupIdsMap);
        Assert.assertNotNull(resultLookupIdMap);
        Assert.assertEquals(resultLookupIdMap.getOrgId(), lookupIdsMap.getOrgId());
        Assert.assertEquals(resultLookupIdMap.getOrgName(), lookupIdsMap.getOrgName());
        Assert.assertEquals(resultLookupIdMap.getExternalSystemType(), lookupIdsMap.getExternalSystemType());
        Assert.assertNotNull(resultLookupIdMap.getId());

        confirmNonEmptyLookupConfigs();
    }

    @Test(groups = "deployment")
    public void updateLookupIdMap() {

        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(mainCustomerSpace,
                null);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() > 0);

        lookupIdConfigs.keySet().stream().forEach(k -> {
            Assert.assertTrue(lookupIdConfigs.get(k).size() > 0);
            lookupIdConfigs.get(k).stream().forEach(c -> {
                Assert.assertNotNull(c);
                Assert.assertNotNull(c.getId());
                Assert.assertNotNull(c.getOrgId());
                Assert.assertNotNull(c.getOrgName());
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
            });
        });

    }

    @Test(groups = "deployment")
    public void getAllLookupIds() {
        Map<String, List<CDLExternalSystemMapping>> allLookupIds = lookupIdMappingProxy
                .getAllLookupIds(mainCustomerSpace, null);
        Assert.assertNotNull(allLookupIds);
        Assert.assertTrue(allLookupIds.keySet().size() > 0);

        allLookupIds.keySet().stream().forEach(k -> {
            CDLExternalSystemType externalSystemType = CDLExternalSystemType.valueOf(k);
            Assert.assertNotNull(externalSystemType);
            Assert.assertTrue(allLookupIds.get(k).size() > 0);
            allLookupIds.get(k).stream().forEach(c -> {
                Assert.assertNotNull(c);
                Assert.assertNotNull(c.getDisplayName());
                Assert.assertNotNull(c.getFieldName());
                Assert.assertNotNull(c.getFieldType());
            });
        });

    }

    @Test(groups = "deployment")
    public void getAllCDLExternalSystemType() {
        List<CDLExternalSystemType> allCDLExternalSystemType = lookupIdMappingProxy
                .getAllCDLExternalSystemType(mainCustomerSpace);
        Assert.assertNotNull(allCDLExternalSystemType);
        Assert.assertTrue(CollectionUtils.isNotEmpty(allCDLExternalSystemType));
    }

    private void confirmNonEmptyLookupConfigs() {
        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(mainCustomerSpace,
                null);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() > 0);

        lookupIdConfigs.keySet().stream().forEach(k -> {
            CDLExternalSystemType externalSystemType = CDLExternalSystemType.valueOf(k);
            Assert.assertTrue(lookupIdConfigs.get(k).size() > 0);
            lookupIdConfigs.get(k).stream().forEach(c -> {
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
            });
        });
    }
}
