package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class LookupIdMappingEntityMgrTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    private LookupIdMap lookupIdMap;
    private String configId;
    private String configIdWithAuth;
    private String configIdWithFieldMapping;
    private String orgId = "ABC_s";
    private String orgName = "n1234_1";
    private String accountId = "someAccId";
    private String description = "some description";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
        List<LookupIdMap> lookupIdsMapping = lookupIdMappingEntityMgr.getLookupIdMappings(null, null, true);
        Assert.assertNotNull(lookupIdsMapping);
        Assert.assertEquals(lookupIdsMapping.size(), 0, JsonUtils.serialize(lookupIdsMapping));
        Assert.assertTrue(CollectionUtils.isEmpty(lookupIdsMapping));
        Assert.assertNull(lookupIdMappingEntityMgr.getLookupIdMap("some_bad_id"));
    }

    @Test(groups = "functional")
    public void testFindWithNoExistingMaps() {
        List<LookupIdMap> lookupIdsMapping = lookupIdMappingEntityMgr.getLookupIdMappings(null, null, true);
        Assert.assertTrue(CollectionUtils.isEmpty(lookupIdsMapping));
    }

    @Test(groups = "functional", dependsOnMethods = { "testFindWithNoExistingMaps" })
    public void testCreate() {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
        lookupIdMap.setOrgId(orgId);
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
        List<LookupIdMap> lookupIdsMapping = lookupIdMappingEntityMgr.getLookupIdMappings(null, null, true);
        Assert.assertTrue(CollectionUtils.isNotEmpty(lookupIdsMapping));

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
        Assert.assertTrue(StringUtils.isBlank(extractedLookupIdMap.getExportFolder()));
    }

    @Test(groups = "functional", dependsOnMethods = { "testFind" })
    public void testCreateWithEmptyOrgName() {
        LookupIdMap emptyOrgNameLookupIdMap = new LookupIdMap();
        emptyOrgNameLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        emptyOrgNameLookupIdMap.setOrgId(orgId + "_different");
        emptyOrgNameLookupIdMap.setOrgName("");
        TestFrameworkUtils.assertThrowsLedpExceptionWithCode(LedpCode.LEDP_40080,
                () -> lookupIdMappingEntityMgr.createExternalSystem(emptyOrgNameLookupIdMap));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWithEmptyOrgName" })
    public void testCreateWithDuplicateOrgName() {
        LookupIdMap duplicateOrgNameLookupIdMap = new LookupIdMap();
        duplicateOrgNameLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        duplicateOrgNameLookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
        duplicateOrgNameLookupIdMap.setOrgId(orgId + "_different");
        duplicateOrgNameLookupIdMap.setOrgName(orgName);
        TestFrameworkUtils.assertThrowsLedpExceptionWithCode(LedpCode.LEDP_40081,
                () -> lookupIdMappingEntityMgr.createExternalSystem(duplicateOrgNameLookupIdMap));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWithDuplicateOrgName" })
    public void testCreateDuplicate() {
        LookupIdMap duplicateLookupIdMap = new LookupIdMap();
        duplicateLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        duplicateLookupIdMap.setOrgId(orgId);
        duplicateLookupIdMap.setOrgName(orgName);
        Assert.assertThrows(() -> lookupIdMappingEntityMgr.createExternalSystem(duplicateLookupIdMap));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateDuplicate" })
    public void testCreateAnother() {
        LookupIdMap anotherLookupIdMap = new LookupIdMap();
        anotherLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        anotherLookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
        anotherLookupIdMap.setOrgId(orgId + "_different");
        anotherLookupIdMap.setOrgName(orgName + "_different");
        Assert.assertNotNull(lookupIdMappingEntityMgr.createExternalSystem(anotherLookupIdMap));
        Assert.assertEquals(anotherLookupIdMap.getIsRegistered(), Boolean.TRUE);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateAnother" })
    public void testUpdate() {
        LookupIdMap extractedLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(configId);

        extractedLookupIdMap.setAccountId(accountId);
        extractedLookupIdMap.setDescription(description);
        lookupIdMappingEntityMgr.updateLookupIdMap(extractedLookupIdMap);
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
        lookupIdMappingEntityMgr.updateLookupIdMap(extractedLookupIdMap);
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
    public void testUpdateToEmptyOrgName() {
        LookupIdMap extractedLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(configId);
        extractedLookupIdMap.setOrgName("");

        TestFrameworkUtils.assertThrowsLedpExceptionWithCode(LedpCode.LEDP_40080,
                () -> lookupIdMappingEntityMgr.updateLookupIdMap(extractedLookupIdMap));
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateToEmptyOrgName" })
    public void testUpdateToDuplicateOrgName() {
        LookupIdMap anotherLookupIdMap = new LookupIdMap();
        anotherLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        anotherLookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
        anotherLookupIdMap.setOrgId(orgId + "_unique");
        anotherLookupIdMap.setOrgName(orgName + "_unique");

        anotherLookupIdMap = lookupIdMappingEntityMgr.createExternalSystem(anotherLookupIdMap);
        String orgNameToCopy = anotherLookupIdMap.getOrgName();

        LookupIdMap extractedLookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(configId);
        extractedLookupIdMap.setOrgName(orgNameToCopy);

        TestFrameworkUtils.assertThrowsLedpExceptionWithCode(LedpCode.LEDP_40081,
                () -> lookupIdMappingEntityMgr.updateLookupIdMap(extractedLookupIdMap));
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateToDuplicateOrgName" })
    public void testDelete() {
        lookupIdMappingEntityMgr.deleteLookupIdMap(configId);
        Assert.assertNull(lookupIdMappingEntityMgr.getLookupIdMap(configId));
    }

    @Test(groups = "functional", dependsOnMethods = { "testFindWithNoExistingMaps" })
    public void testCreateWithAuthentication() {
        LookupIdMap lookupIdMapWithAuth = new LookupIdMap();
        lookupIdMapWithAuth.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMapWithAuth.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMapWithAuth.setOrgId("Marketo_AuthTest");
        lookupIdMapWithAuth.setOrgName("Marketo_AuthTest");

        ExternalSystemAuthentication externalAuth = new ExternalSystemAuthentication();
        externalAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        externalAuth.setSolutionInstanceId(UUID.randomUUID().toString());
        lookupIdMapWithAuth.setExternalAuthentication(externalAuth);

        LookupIdMap lookupIdWithAuthCreated = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMapWithAuth);
        assertNotNull(lookupIdWithAuthCreated);
        assertNotNull(lookupIdWithAuthCreated.getId());
        configIdWithAuth = lookupIdWithAuthCreated.getId();
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWithAuthentication" })
    public void testFindWithAuthentication() {
        LookupIdMap lookupIdWithAuth = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithAuth);
        validateLookupIdWithAuth(lookupIdWithAuth);

        LookupIdMap lookupIdAuthByOrgId = lookupIdMappingEntityMgr.getLookupIdMap(lookupIdWithAuth.getOrgId(),
                lookupIdWithAuth.getExternalSystemType());
        validateLookupIdWithAuth(lookupIdAuthByOrgId);
        assertEquals(lookupIdWithAuth.getExternalAuthentication().getTrayAuthenticationId(),
                lookupIdAuthByOrgId.getExternalAuthentication().getTrayAuthenticationId());
    }

    private void validateLookupIdWithAuth(LookupIdMap lookupIdWithAuth) {
        assertNotNull(lookupIdWithAuth);
        assertNotNull(lookupIdWithAuth.getExternalAuthentication());
        ExternalSystemAuthentication externalAuthFromDB = lookupIdWithAuth.getExternalAuthentication();
        assertNotNull(externalAuthFromDB.getId());
        assertNotNull(externalAuthFromDB.getTrayAuthenticationId());
        assertNotNull(externalAuthFromDB.getSolutionInstanceId());
        assertFalse(externalAuthFromDB.getTrayWorkflowEnabled());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWithAuthentication" })
    public void testUpdateWithAuthentication() {
        LookupIdMap lookupIdWithAuth = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithAuth);
        assertNotNull(lookupIdWithAuth);
        assertNotNull(lookupIdWithAuth.getExternalAuthentication());
        ExternalSystemAuthentication prevExtAuth = lookupIdWithAuth.getExternalAuthentication();
        ExternalSystemAuthentication updatedExtAuth = new ExternalSystemAuthentication();
        updatedExtAuth.setId(lookupIdWithAuth.getExternalAuthentication().getId());
        updatedExtAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        updatedExtAuth.setSolutionInstanceId(UUID.randomUUID().toString());
        updatedExtAuth.setTrayWorkflowEnabled(true);
        lookupIdWithAuth.setExternalAuthentication(updatedExtAuth);

        lookupIdMappingEntityMgr.updateLookupIdMap(lookupIdWithAuth);

        LookupIdMap lookupIdWithAuthFromDB = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithAuth);
        assertNotNull(lookupIdWithAuthFromDB.getId());
        assertNotNull(updatedExtAuth);
        assertNotNull(updatedExtAuth.getId());
        assertNotNull(updatedExtAuth.getTrayAuthenticationId());
        assertNotNull(updatedExtAuth.getSolutionInstanceId());
        assertTrue(updatedExtAuth.getTrayWorkflowEnabled());
        assertNotEquals(prevExtAuth.getTrayAuthenticationId(), updatedExtAuth.getTrayAuthenticationId());
        assertNotEquals(prevExtAuth.getSolutionInstanceId(), updatedExtAuth.getSolutionInstanceId());
        assertTrue(updatedExtAuth.getTrayWorkflowEnabled());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateWithAuthentication" })
    public void testUpdateWithNull() {
        LookupIdMap lookupIdWithAuth = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithAuth);
        assertNotNull(lookupIdWithAuth);
        assertNotNull(lookupIdWithAuth.getExternalAuthentication());
        ExternalSystemAuthentication updatedExtAuth = new ExternalSystemAuthentication();
        updatedExtAuth.setId(lookupIdWithAuth.getExternalAuthentication().getId());
        updatedExtAuth.setTrayAuthenticationId(null);
        updatedExtAuth.setSolutionInstanceId(null);
        lookupIdWithAuth.setExternalAuthentication(updatedExtAuth);

        lookupIdMappingEntityMgr.updateLookupIdMap(lookupIdWithAuth);
        LookupIdMap lookupIdWithAuthFromDB = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithAuth);
        updatedExtAuth = lookupIdWithAuthFromDB.getExternalAuthentication();
        assertNotNull(lookupIdWithAuthFromDB.getId());
        assertNotNull(updatedExtAuth);
        assertNotNull(updatedExtAuth.getId());
        assertNull(updatedExtAuth.getTrayAuthenticationId());
        assertNull(updatedExtAuth.getSolutionInstanceId());
        assertFalse(updatedExtAuth.getTrayWorkflowEnabled());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateWithNull" })
    public void testCreateWithFieldMapping() {
        LookupIdMap lookupIdMapWithFieldMapping = new LookupIdMap();
        lookupIdMapWithFieldMapping.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMapWithFieldMapping.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMapWithFieldMapping.setOrgId("Marketo_FieldMappingTest");
        lookupIdMapWithFieldMapping.setOrgName("Marketo_FieldMappingTest");

        List<ExportFieldMetadataMapping> exportFieldMappings = new ArrayList<>();
        exportFieldMappings.add(new ExportFieldMetadataMapping("COMPANY_NAME", "company", false));
        exportFieldMappings.add(new ExportFieldMetadataMapping("Email", "email", false));
        exportFieldMappings.add(new ExportFieldMetadataMapping("Phone", "phone", false));

        LookupIdMap lookupIdMapWithFieldMappingCreated = lookupIdMappingEntityMgr
                .createExternalSystem(lookupIdMapWithFieldMapping);
        assertNotNull(lookupIdMapWithFieldMappingCreated);
        assertNotNull(lookupIdMapWithFieldMappingCreated.getId());
        configIdWithFieldMapping = lookupIdMapWithFieldMappingCreated.getId();
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWithFieldMapping" })
    public void testUpdateWithFieldMapping() {
        LookupIdMap lookupIdMapWithFieldMapping = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithFieldMapping);
        assertNotNull(lookupIdMapWithFieldMapping);
        List<ExportFieldMetadataMapping> existingFieldMapping = lookupIdMapWithFieldMapping
                .getExportFieldMetadataMappings();
        assertNotNull(existingFieldMapping);

        List<ExportFieldMetadataMapping> updatedFieldMapping = new ArrayList<>();
        updatedFieldMapping.add(new ExportFieldMetadataMapping("COMPANY_NAME", "company", false));
        updatedFieldMapping.add(new ExportFieldMetadataMapping("Address", "address", false));
        updatedFieldMapping.add(new ExportFieldMetadataMapping("ZipCode", "zipcode", false));
        lookupIdMapWithFieldMapping.setExportFieldMappings(updatedFieldMapping);

        SleepUtils.sleep(2000L);

        lookupIdMappingEntityMgr.updateLookupIdMap(lookupIdMapWithFieldMapping);

        LookupIdMap lookupIdWithFieldMappingFromDB = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithFieldMapping);
        assertNotNull(lookupIdWithFieldMappingFromDB.getId());
        List<ExportFieldMetadataMapping> retrievedFieldMapping = lookupIdWithFieldMappingFromDB
                .getExportFieldMetadataMappings();
        assertNotNull(retrievedFieldMapping);
        assertEquals(retrievedFieldMapping.size(), 3);

        List<String> sourceFields = retrievedFieldMapping.stream().map(ExportFieldMetadataMapping::getSourceField)
                .collect(Collectors.toList());
        assertTrue(sourceFields.contains("COMPANY_NAME"));
        assertTrue(sourceFields.contains("Address"));
        assertTrue(sourceFields.contains("ZipCode"));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateWithAuthentication" })
    public void testGetLookupIdMapByExtSysAuth() {
        LookupIdMap lookupIdWithAuth = lookupIdMappingEntityMgr.getLookupIdMap(configIdWithAuth);
        validateLookupIdWithAuth(lookupIdWithAuth);
        ExternalSystemAuthentication extSysAuth = lookupIdWithAuth.getExternalAuthentication();

        LookupIdMap lookupIdMapByExtSysAuth = lookupIdMappingEntityMgr.retrieveLookupIdMapByExtSysAuth(extSysAuth.getId());
        validateLookupIdWithAuth(lookupIdMapByExtSysAuth);
        assertEquals(lookupIdWithAuth.getPid(), lookupIdMapByExtSysAuth.getPid());
    }
}
