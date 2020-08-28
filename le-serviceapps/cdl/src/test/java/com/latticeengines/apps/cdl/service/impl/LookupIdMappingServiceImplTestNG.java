package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.core.entitymgr.DropBoxEntityMgr;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.remote.tray.TraySettings;
import com.latticeengines.remote.exposed.service.tray.TrayService;

public class LookupIdMappingServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Mock
    private TrayService trayService;

    @Inject
    @InjectMocks
    private LookupIdMappingService lookupIdMappingLaunchService;

    @Inject
    private DropBoxEntityMgr dropBoxEntityMgr;

    @Value("${aws.customer.s3.bucket}")
    private String s3CustomerBucket;

    private String dropBox;
    private TraySettings validSettings;
    private TraySettings faultyAuthOnly;
    private TraySettings faultySolInstanceOnly;
    
    private String userToken = "userToken";
    private String validAuth = "validAuth";
    private String invalidAuth = "invalidAuth";
    private String validSolInstance = "validSolInstance";
    private String invalidSolInstance = "invalidSolInstance";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        setupTestEnvironment();
        validSettings = new TraySettings(userToken, validAuth, validSolInstance, null);
        faultyAuthOnly = new TraySettings(userToken, invalidAuth, validSolInstance, null);
        faultySolInstanceOnly = new TraySettings(userToken, validAuth, invalidSolInstance, null);
        dropBox = dropBoxEntityMgr.createDropBox(mainTestTenant, "us-east-1").getDropBox();
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
        Assert.assertTrue(lookupIdsMapping.containsKey(CDLExternalSystemType.FILE_SYSTEM.name()));
        Assert.assertEquals(lookupIdsMapping.get(CDLExternalSystemType.FILE_SYSTEM.name()).size(), 1);
        Assert.assertEquals(lookupIdsMapping.get(CDLExternalSystemType.FILE_SYSTEM.name()).get(0).getExportFolder(),
                String.format("%s/dropfolder/%s/export/campaigns", s3CustomerBucket, dropBox));
        // "");
        Assert.assertNull(lookupIdMappingLaunchService.getLookupIdMap("some_bad_id"));
        String orgId = "ABC_s";
        String orgName = "n1234_1";

        LookupIdMap extractedLookupIdMap = testRegisterDeregister(orgId, orgName, true, null);

        String configId = extractedLookupIdMap.getId();

        extractedLookupIdMap = testRegisterDeregister(orgId, orgName, true, null);

        LookupIdMap duplicateLookupIdMap = new LookupIdMap();
        duplicateLookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        duplicateLookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
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
        duplicateLookupIdMap.setOrgName(orgName + "_different");
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
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
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

    @Test(groups = "functional")
    public void testDeleteConnection() {
        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Salesforce);
        ExternalSystemAuthentication sysAuth = new ExternalSystemAuthentication();
        sysAuth.setSolutionInstanceId(validSolInstance);
        sysAuth.setTrayAuthenticationId(validAuth);
        lookupIdMap.setExternalAuthentication(sysAuth);
        lookupIdMap.setOrgId("orgId");
        lookupIdMap.setOrgName("orgName");

        lookupIdMappingLaunchService.registerExternalSystem(lookupIdMap);
        String configId = lookupIdMap.getId();

        when(trayService.removeSolutionInstance(validSettings)).thenReturn(null);
        when(trayService.removeAuthentication(validSettings)).thenReturn(null);

        lookupIdMappingLaunchService.deleteConnection(configId, validSettings);
        Assert.assertNull(lookupIdMappingLaunchService.getLookupIdMap(configId));

        LookupIdMap lookupIdMap2 = new LookupIdMap();
        lookupIdMap2.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap2.setExternalSystemName(CDLExternalSystemName.Salesforce);
        ExternalSystemAuthentication sysAuth2 = new ExternalSystemAuthentication();
        sysAuth2.setSolutionInstanceId(validSolInstance);
        sysAuth2.setTrayAuthenticationId(validAuth);
        lookupIdMap2.setExternalAuthentication(sysAuth2);
        lookupIdMap2.setOrgId("orgId");
        lookupIdMap2.setOrgName("orgName");

        lookupIdMappingLaunchService.registerExternalSystem(lookupIdMap2);
        String configId2 = lookupIdMap2.getId();

        doThrow(new LedpException(LedpCode.LEDP_31000, new String[] { "error" })).when(trayService)
                .removeSolutionInstance(faultySolInstanceOnly);
        when(trayService.removeAuthentication(faultySolInstanceOnly)).thenReturn(null);

        try {
            lookupIdMappingLaunchService.deleteConnection(configId2, faultySolInstanceOnly);
            Assert.fail("delete connection should fail");
        } catch (LedpException ex) {
            Assert.assertNotNull(lookupIdMappingLaunchService.getLookupIdMap(configId2));
            Assert.assertNotNull(lookupIdMappingLaunchService.getLookupIdMap(configId2).getExternalAuthentication()
                    .getSolutionInstanceId());
            Assert.assertNotNull(lookupIdMappingLaunchService.getLookupIdMap(configId2).getExternalAuthentication()
                    .getTrayAuthenticationId());
        }

        when(trayService.removeSolutionInstance(faultyAuthOnly)).thenReturn(null);
        doThrow(new LedpException(LedpCode.LEDP_31000, new String[] { "error" })).when(trayService)
                .removeAuthentication(faultyAuthOnly);

        try {
            lookupIdMappingLaunchService.deleteConnection(configId2, faultyAuthOnly);
            Assert.fail("delete connection should fail");
        } catch (LedpException ex) {
            Assert.assertNotNull(lookupIdMappingLaunchService.getLookupIdMap(configId2));
            Assert.assertNull(lookupIdMappingLaunchService.getLookupIdMap(configId2).getExternalAuthentication()
                    .getSolutionInstanceId());
            Assert.assertNotNull(lookupIdMappingLaunchService.getLookupIdMap(configId2).getExternalAuthentication()
                    .getTrayAuthenticationId());

            lookupIdMappingLaunchService.deleteConnection(configId2, validSettings);
            Assert.assertNull(lookupIdMappingLaunchService.getLookupIdMap(configId2));
        }
    }
}
