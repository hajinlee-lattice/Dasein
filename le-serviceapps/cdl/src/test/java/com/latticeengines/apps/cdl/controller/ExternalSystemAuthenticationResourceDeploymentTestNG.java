package com.latticeengines.apps.cdl.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.proxy.exposed.cdl.ExternalSystemAuthenticationProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;

public class ExternalSystemAuthenticationResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Inject
    private ExternalSystemAuthenticationProxy extSysAuthenticationProxy;

    private LookupIdMap lookupIdMappingRef = null;
    private ExternalSystemAuthentication extSysAuthenticationRef = null;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void verifyBaseSetup() {
        Map<String, List<LookupIdMap>> lookupIdConfigs = lookupIdMappingProxy.getLookupIdsMapping(mainCustomerSpace,
                null, null, true);
        assertNotNull(lookupIdConfigs);
        assertEquals(lookupIdConfigs.keySet().size(), 1);
        assertTrue(lookupIdConfigs.containsKey(CDLExternalSystemType.FILE_SYSTEM.name()));

        List<ExternalSystemAuthentication> extSysAuthLst = extSysAuthenticationProxy
                .findAuthentications(mainCustomerSpace);
        assertNotNull(extSysAuthLst);
        assertEquals(extSysAuthLst.size(), 0);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "verifyBaseSetup")
    public void testCreateAuthentication() {
        LookupIdMap lookupIdsMap = new LookupIdMap();
        lookupIdsMap.setOrgId("Org_" + System.currentTimeMillis());
        lookupIdsMap.setOrgName(CDLExternalSystemName.Marketo + "_" + lookupIdsMap.getOrgId());
        lookupIdsMap.setExternalSystemType(CDLExternalSystemType.MAP);
        LookupIdMap resultLookupIdMap = lookupIdMappingProxy.registerExternalSystem(mainCustomerSpace, lookupIdsMap);
        assertNotNull(resultLookupIdMap);
        assertNotNull(resultLookupIdMap.getId());
        lookupIdMappingRef = resultLookupIdMap;

        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setLookupMapConfigId(resultLookupIdMap.getId());
        extSysAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        extSysAuth = extSysAuthenticationProxy.createAuthentication(mainCustomerSpace, extSysAuth);
        extSysAuthenticationRef = extSysAuth;
        verifyCurrentAuthentication(extSysAuth);
    }

    private void verifyCurrentAuthentication(ExternalSystemAuthentication extSysAuth) {
        assertNotNull(extSysAuth);
        assertNotNull(extSysAuth.getId());
        assertNotNull(extSysAuth.getLookupMapConfigId());
        assertEquals(extSysAuth.getLookupMapConfigId(), lookupIdMappingRef.getId());
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testCreateAuthentication")
    public void testFindAuthentication_ByAuthID() {
        ExternalSystemAuthentication extSysAuth = extSysAuthenticationProxy
                .findAuthenticationByAuthId(mainCustomerSpace, extSysAuthenticationRef.getId());
        verifyCurrentAuthentication(extSysAuth);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testFindAuthentication_ByAuthID")
    public void testUpdateAuthentication() {
        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setId(extSysAuthenticationRef.getId());
        extSysAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        extSysAuth = extSysAuthenticationProxy.updateAuthentication(mainCustomerSpace, extSysAuth.getId(), extSysAuth);
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            // Ignore
        }
        verifyCurrentAuthentication(extSysAuth);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testUpdateAuthentication")
    public void testFindAllAuthentications() {
        List<ExternalSystemAuthentication> extSysAuthLst = extSysAuthenticationProxy
                .findAuthentications(mainCustomerSpace);
        assertNotNull(extSysAuthLst);
        assertTrue(extSysAuthLst.size() == 1);
        ExternalSystemAuthentication currAuth = extSysAuthLst.get(0);
        verifyCurrentAuthentication(currAuth);
        assertEquals(currAuth.getId(), extSysAuthenticationRef.getId());
        // As we have called Update before this test, we should have different
        // TrayAuthID
        assertNotEquals(currAuth.getTrayAuthenticationId(), extSysAuthenticationRef.getTrayAuthenticationId());
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testUpdateAuthentication")
    public void testFindAuthenticationsByLookupMappingId() {
        List<String> lookupMappingIds = Arrays.asList(new String[] { lookupIdMappingRef.getId() });
        List<ExternalSystemAuthentication> extSysAuthLst = extSysAuthenticationProxy
                .findAuthenticationsByLookupMapIds(mainCustomerSpace, lookupMappingIds);
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
