package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public class LookupIdMappingEntityMgrTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    private LookupIdMap lookupIdMap;
    private String configId;
    private String orgId = "ABC_s";
    private String orgName = "n1234_1";
    private String accountId = "someAccId";
    private String description = "some description";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
        Map<String, List<LookupIdMap>> lookupIdsMapping = lookupIdMappingEntityMgr.getLookupIdsMapping(null, null,
                true);
        Assert.assertNotNull(lookupIdsMapping);
        Assert.assertTrue(lookupIdsMapping.size() == 0, JsonUtils.serialize(lookupIdsMapping));
        Assert.assertTrue(MapUtils.isEmpty(lookupIdsMapping));
        Assert.assertNull(lookupIdMappingEntityMgr.getLookupIdMap("some_bad_id"));
    }

    @Test(groups = "functional")
    public void testCreate() {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
        lookupIdMap.setOrgId(orgId);
        lookupIdMap.setOrgName(orgName);
        lookupIdMap.setOrgName(orgName);
        lookupIdMap = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap);
        Assert.assertNotNull(lookupIdMap);
        Assert.assertNull(lookupIdMap.getAccountId());
        Assert.assertNull(lookupIdMap.getDescription());
        Assert.assertNotNull(lookupIdMap.getId());
        Assert.assertNotNull(lookupIdMap.getCreated());
        Assert.assertNotNull(lookupIdMap.getUpdated());
        Assert.assertEquals(lookupIdMap.getExternalSystemType(), CDLExternalSystemType.CRM);
        Assert.assertEquals(lookupIdMap.getExternalSystemName(), CDLExternalSystemName.Salesforce);
        Assert.assertEquals(lookupIdMap.getOrgId(), orgId);
        Assert.assertEquals(lookupIdMap.getOrgName(), orgName);
        Assert.assertEquals(lookupIdMap.getIsRegistered(), Boolean.TRUE);

        configId = lookupIdMap.getId();
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreate" })
    public void testFind() {
        Map<String, List<LookupIdMap>> lookupIdsMapping = lookupIdMappingEntityMgr.getLookupIdsMapping(null, null,
                true);
        Assert.assertTrue(MapUtils.isNotEmpty(lookupIdsMapping));

        LookupIdMap extractedLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(configId);
        Assert.assertNotNull(extractedLookupIdMap);
        Assert.assertNull(extractedLookupIdMap.getAccountId());
        Assert.assertNull(extractedLookupIdMap.getDescription());
        Assert.assertEquals(extractedLookupIdMap.getId(), lookupIdMap.getId());
        Assert.assertNotNull(extractedLookupIdMap.getCreated());
        Assert.assertNotNull(extractedLookupIdMap.getUpdated());
        Assert.assertEquals(extractedLookupIdMap.getExternalSystemType(), CDLExternalSystemType.CRM);
        Assert.assertEquals(extractedLookupIdMap.getExternalSystemName(), CDLExternalSystemName.Salesforce);
        Assert.assertEquals(extractedLookupIdMap.getOrgId(), orgId);
        Assert.assertEquals(extractedLookupIdMap.getOrgName(), orgName);
    }

    @Test(groups = "functional", dependsOnMethods = { "testFind" })
    public void testCreateDuplicate() {
        LookupIdMap duplicateLookupIdMap = new LookupIdMap();
        duplicateLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        duplicateLookupIdMap.setOrgId(orgId);
        duplicateLookupIdMap.setOrgName(orgName);
        try {
            lookupIdMappingEntityMgr.createExternalSystem(duplicateLookupIdMap);
            Assert.fail("Should not be able to create duplicate entry");
        } catch (Exception ex) {
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateDuplicate" })
    public void testCreateAnother() {
        LookupIdMap anotherLookupIdMap = new LookupIdMap();
        anotherLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        anotherLookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
        anotherLookupIdMap.setOrgId(orgId + "_different");
        anotherLookupIdMap.setOrgName(orgName);
        Assert.assertNotNull(lookupIdMappingEntityMgr.createExternalSystem(anotherLookupIdMap));
        Assert.assertEquals(anotherLookupIdMap.getIsRegistered(), Boolean.TRUE);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateAnother" })
    public void testUpdate() {
        LookupIdMap extractedLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(configId);

        extractedLookupIdMap.setAccountId(accountId);
        extractedLookupIdMap.setDescription(description);
        lookupIdMappingEntityMgr.updateLookupIdMap(configId, extractedLookupIdMap);
        LookupIdMap extractedLookupIdMap2 = lookupIdMappingEntityMgr.getLookupIdMap(configId);
        Assert.assertNotNull(extractedLookupIdMap2);
        Assert.assertEquals(extractedLookupIdMap2.getAccountId(), accountId);
        Assert.assertEquals(extractedLookupIdMap2.getDescription(), description);
        Assert.assertEquals(extractedLookupIdMap2.getId(), extractedLookupIdMap.getId());
        Assert.assertNotNull(extractedLookupIdMap2.getCreated());
        Assert.assertNotNull(extractedLookupIdMap2.getUpdated());
        Assert.assertEquals(extractedLookupIdMap2.getExternalSystemType(), CDLExternalSystemType.CRM);
        Assert.assertEquals(extractedLookupIdMap2.getOrgId(), orgId);
        Assert.assertEquals(extractedLookupIdMap2.getOrgName(), orgName);
        Assert.assertEquals(extractedLookupIdMap2.getIsRegistered(), Boolean.TRUE);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testUpdate2() {
        LookupIdMap extractedLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(configId);

        extractedLookupIdMap.setAccountId(accountId);
        extractedLookupIdMap.setDescription(description);
        extractedLookupIdMap.setIsRegistered(Boolean.FALSE);
        lookupIdMappingEntityMgr.updateLookupIdMap(configId, extractedLookupIdMap);
        LookupIdMap extractedLookupIdMap2 = lookupIdMappingEntityMgr.getLookupIdMap(configId);
        Assert.assertNotNull(extractedLookupIdMap2);
        Assert.assertEquals(extractedLookupIdMap2.getAccountId(), accountId);
        Assert.assertEquals(extractedLookupIdMap2.getDescription(), description);
        Assert.assertEquals(extractedLookupIdMap2.getId(), extractedLookupIdMap.getId());
        Assert.assertNotNull(extractedLookupIdMap2.getCreated());
        Assert.assertNotNull(extractedLookupIdMap2.getUpdated());
        Assert.assertEquals(extractedLookupIdMap2.getExternalSystemType(), CDLExternalSystemType.CRM);
        Assert.assertEquals(extractedLookupIdMap2.getExternalSystemName(), CDLExternalSystemName.Salesforce);
        Assert.assertEquals(extractedLookupIdMap2.getOrgId(), orgId);
        Assert.assertEquals(extractedLookupIdMap2.getOrgName(), orgName);
        Assert.assertEquals(extractedLookupIdMap2.getIsRegistered(), Boolean.FALSE);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate2" })
    public void testDelete() {
        lookupIdMappingEntityMgr.deleteLookupIdMap(configId);
        Assert.assertNull(lookupIdMappingEntityMgr.getLookupIdMap(configId));
    }

}
