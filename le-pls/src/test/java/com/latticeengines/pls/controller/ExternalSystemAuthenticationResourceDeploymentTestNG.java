package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;

public class ExternalSystemAuthenticationResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    private LookupIdMap lookupIdMappingRef = null;
    private ExternalSystemAuthentication extSysAuthenticationRef = null;
    private String REST_RESOURCE = null;
    private String TEST_TENANT_ID = null;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        REST_RESOURCE = getRestAPIHostPort() + "/pls/external-system-authentication";
        TEST_TENANT_ID = testBed.getMainTestTenant().getId();
    }

    @Test(groups = "deployment")
    public void verifyBaseSetup() {
        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(TEST_TENANT_ID, null,
                null, true);
        assertNotNull(lookupIdConfigs);
        assertEquals(lookupIdConfigs.keySet().size(), 1);
        assertTrue(lookupIdConfigs.containsKey(CDLExternalSystemType.FILE_SYSTEM.name()));

        List<?> listObject = restTemplate.getForObject(REST_RESOURCE + "/", List.class);
        List<ExternalSystemAuthentication> extSysAuthLst = JsonUtils.convertList(listObject,
                ExternalSystemAuthentication.class);
        assertNotNull(extSysAuthLst);
        assertTrue(extSysAuthLst.size() == 0);
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyBaseSetup")
    public void testCreateAuthentication() {
        LookupIdMap lookupIdsMap = new LookupIdMap();
        lookupIdsMap.setOrgId("Org_" + System.currentTimeMillis());
        lookupIdsMap.setOrgName(CDLExternalSystemName.Marketo + "_" + lookupIdsMap.getOrgId());
        lookupIdsMap.setExternalSystemType(CDLExternalSystemType.MAP);
        LookupIdMap resultLookupIdMap = lookupIdMappingProxy.registerExternalSystem(TEST_TENANT_ID, lookupIdsMap);
        assertNotNull(resultLookupIdMap);
        assertNotNull(resultLookupIdMap.getId());
        lookupIdMappingRef = resultLookupIdMap;

        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setLookupMapConfigId(resultLookupIdMap.getId());
        extSysAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        extSysAuth = restTemplate.postForObject(REST_RESOURCE + "/", extSysAuth, ExternalSystemAuthentication.class);
        extSysAuthenticationRef = extSysAuth;
        verifyCurrentAuthentication(extSysAuth);
    }

    private void verifyCurrentAuthentication(ExternalSystemAuthentication extSysAuth) {
        assertNotNull(extSysAuth);
        assertNotNull(extSysAuth.getId());
        assertNotNull(extSysAuth.getLookupMapConfigId());
        assertEquals(extSysAuth.getLookupMapConfigId(), lookupIdMappingRef.getId());
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateAuthentication")
    public void testFindAuthentication_ByAuthID() {
        ExternalSystemAuthentication extSysAuth = restTemplate.getForObject(
                REST_RESOURCE + "/" + extSysAuthenticationRef.getId(), ExternalSystemAuthentication.class);
        verifyCurrentAuthentication(extSysAuth);
    }

    @Test(groups = "deployment", dependsOnMethods = "testFindAuthentication_ByAuthID")
    public void testUpdateAuthentication() {
        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setId(extSysAuthenticationRef.getId());
        extSysAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        restTemplate.put(REST_RESOURCE + "/" + extSysAuthenticationRef.getId(), extSysAuth);
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            // Ignore
        }
        ExternalSystemAuthentication updatedExtSysAuth = restTemplate.getForObject(
                REST_RESOURCE + "/" + extSysAuthenticationRef.getId(), ExternalSystemAuthentication.class);
        verifyCurrentAuthentication(updatedExtSysAuth);
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateAuthentication")
    public void testFindAllAuthentications() {
        List<?> listObject = restTemplate.getForObject(REST_RESOURCE + "/", List.class);
        List<ExternalSystemAuthentication> extSysAuthLst = JsonUtils.convertList(listObject,
                ExternalSystemAuthentication.class);
        assertNotNull(extSysAuthLst);
        assertTrue(extSysAuthLst.size() == 1);
        ExternalSystemAuthentication currAuth = extSysAuthLst.get(0);
        verifyCurrentAuthentication(currAuth);
        assertEquals(currAuth.getId(), extSysAuthenticationRef.getId());
        // As we have called Update before this test, we should have different
        // TrayAuthID
        assertNotEquals(currAuth.getTrayAuthenticationId(), extSysAuthenticationRef.getTrayAuthenticationId());
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateAuthentication")
    public void testFindAuthenticationsByLookupMappingId() {
        List<?> listObject = restTemplate.getForObject(
                REST_RESOURCE + "/lookupid-mappings?mapping_ids=" + lookupIdMappingRef.getId(), List.class);
        List<ExternalSystemAuthentication> extSysAuthLst = JsonUtils.convertList(listObject,
                ExternalSystemAuthentication.class);
        assertNotNull(extSysAuthLst);
        assertTrue(extSysAuthLst.size() == 1);
        ExternalSystemAuthentication currAuth = extSysAuthLst.get(0);
        verifyCurrentAuthentication(currAuth);
        assertEquals(currAuth.getId(), extSysAuthenticationRef.getId());
        // As we have called Update before this test, we should have different
        // TrayAuthID
        assertNotEquals(currAuth.getTrayAuthenticationId(), extSysAuthenticationRef.getTrayAuthenticationId());
    }

}
