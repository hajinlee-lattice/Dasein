package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public class LookupIdMappingServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private LookupIdMappingService lookupIdMappingLaunchService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testBasicOperations() {
        Assert.assertTrue(CollectionUtils.isNotEmpty(lookupIdMappingLaunchService.getAllCDLExternalSystemType()));
        // Assert.assertTrue(MapUtils.isNotEmpty(lookupIdMappingLaunchService.getAllLookupIds(null)));
        Map<String, List<LookupIdMap>> lookupIdsMapping = lookupIdMappingLaunchService.getLookupIdsMapping(null, null,
                true);
        Assert.assertNotNull(lookupIdsMapping);
        Assert.assertEquals(lookupIdsMapping.size(), 1, JsonUtils.serialize(lookupIdsMapping));
        Assert.assertTrue(MapUtils.isNotEmpty(lookupIdsMapping));
        Assert.assertNull(lookupIdMappingLaunchService.getLookupIdMap("some_bad_id"));
        String orgId = "ABC_s";
        String orgName = "n1234_1";

        LookupIdMap extractedLookupIdMap = testRegisterDeregister(orgId, orgName, true, null);

        String configId = extractedLookupIdMap.getId();

        extractedLookupIdMap = testRegisterDeregister(orgId, orgName, true, null);

        LookupIdMap duplicateLookupIdMap = new LookupIdMap();
        duplicateLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        duplicateLookupIdMap.setOrgId(orgId);
        duplicateLookupIdMap.setOrgName(orgName);
        // should be able to upsert duplicate entry
        LookupIdMap duplicateLookupIdMap2 = lookupIdMappingLaunchService.registerExternalSystem(duplicateLookupIdMap);
        Assert.assertEquals(duplicateLookupIdMap2.getId(), extractedLookupIdMap.getId());
        Assert.assertEquals(duplicateLookupIdMap2.getAccountId(), extractedLookupIdMap.getAccountId());
        Assert.assertEquals(duplicateLookupIdMap2.getDescription(), extractedLookupIdMap.getDescription());
        Assert.assertEquals(duplicateLookupIdMap2.getOrgId(), duplicateLookupIdMap.getOrgId());
        Assert.assertEquals(duplicateLookupIdMap2.getOrgName(), duplicateLookupIdMap.getOrgName());
        Assert.assertEquals(duplicateLookupIdMap2.getCreated(), extractedLookupIdMap.getCreated());
        Assert.assertEquals(duplicateLookupIdMap2.getIsRegistered(), extractedLookupIdMap.getIsRegistered());
        Assert.assertEquals(duplicateLookupIdMap2.getExternalSystemType(),
                duplicateLookupIdMap.getExternalSystemType());
        Assert.assertNotEquals(duplicateLookupIdMap2.getUpdated(), extractedLookupIdMap.getUpdated());

        duplicateLookupIdMap.setOrgId(orgId + "_different");
        Assert.assertNotNull(lookupIdMappingLaunchService.registerExternalSystem(duplicateLookupIdMap));

        String accountId = "someAccId";
        extractedLookupIdMap.setAccountId(accountId);
        String description = "some description";
        extractedLookupIdMap.setDescription(description);
        lookupIdMappingLaunchService.updateLookupIdMap(configId, extractedLookupIdMap);
        LookupIdMap extractedLookupIdMap2 = lookupIdMappingLaunchService.getLookupIdMap(configId);
        Assert.assertNotNull(extractedLookupIdMap2);
        Assert.assertEquals(extractedLookupIdMap2.getAccountId(), accountId);
        Assert.assertEquals(extractedLookupIdMap2.getDescription(), description);
        Assert.assertEquals(extractedLookupIdMap2.getId(), extractedLookupIdMap.getId());
        Assert.assertNotNull(extractedLookupIdMap2.getCreated());
        Assert.assertNotNull(extractedLookupIdMap2.getUpdated());
        Assert.assertEquals(extractedLookupIdMap2.getExternalSystemType(), CDLExternalSystemType.CRM);
        Assert.assertEquals(extractedLookupIdMap2.getOrgId(), orgId);
        Assert.assertEquals(extractedLookupIdMap2.getOrgName(), orgName);

        lookupIdMappingLaunchService.deleteLookupIdMap(configId);
        Assert.assertNull(lookupIdMappingLaunchService.getLookupIdMap(configId));
    }

    private LookupIdMap testRegisterDeregister(String orgId, String orgName, boolean doRegister, String configId) {
        Map<String, List<LookupIdMap>> lookupIdsMapping;
        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap.setOrgId(orgId);
        lookupIdMap.setOrgName(orgName);
        if (doRegister) {
            lookupIdMap = lookupIdMappingLaunchService.registerExternalSystem(lookupIdMap);

            Assert.assertNotNull(lookupIdMap);
            Assert.assertNull(lookupIdMap.getAccountId());
            Assert.assertNull(lookupIdMap.getDescription());
            Assert.assertNotNull(lookupIdMap.getId());
            Assert.assertNotNull(lookupIdMap.getCreated());
            Assert.assertNotNull(lookupIdMap.getUpdated());
            Assert.assertEquals(lookupIdMap.getExternalSystemType(), CDLExternalSystemType.CRM);
            Assert.assertEquals(lookupIdMap.getOrgId(), orgId);
            Assert.assertEquals(lookupIdMap.getOrgName(), orgName);
            Assert.assertNotNull(lookupIdMap.getIsRegistered());
            Assert.assertEquals(lookupIdMap.getIsRegistered(), Boolean.TRUE);
        } else {
            lookupIdMappingLaunchService.deregisterExternalSystem(lookupIdMap);
        }

        lookupIdsMapping = lookupIdMappingLaunchService.getLookupIdsMapping(null, null, true);
        Assert.assertTrue(MapUtils.isNotEmpty(lookupIdsMapping));

        LookupIdMap extractedLookupIdMap = lookupIdMappingLaunchService
                .getLookupIdMap(doRegister ? lookupIdMap.getId() : configId);
        Assert.assertNotNull(extractedLookupIdMap);
        Assert.assertNull(extractedLookupIdMap.getAccountId());
        Assert.assertNull(extractedLookupIdMap.getDescription());
        Assert.assertEquals(extractedLookupIdMap.getId(), doRegister ? lookupIdMap.getId() : configId);
        Assert.assertNotNull(extractedLookupIdMap.getCreated());
        Assert.assertNotNull(extractedLookupIdMap.getUpdated());
        Assert.assertEquals(extractedLookupIdMap.getExternalSystemType(), CDLExternalSystemType.CRM);
        Assert.assertEquals(extractedLookupIdMap.getOrgId(), orgId);
        Assert.assertEquals(extractedLookupIdMap.getOrgName(), orgName);
        Assert.assertNotNull(extractedLookupIdMap.getIsRegistered());
        Assert.assertEquals(extractedLookupIdMap.getIsRegistered(), doRegister ? Boolean.TRUE : Boolean.FALSE);
        return extractedLookupIdMap;
    }
}
