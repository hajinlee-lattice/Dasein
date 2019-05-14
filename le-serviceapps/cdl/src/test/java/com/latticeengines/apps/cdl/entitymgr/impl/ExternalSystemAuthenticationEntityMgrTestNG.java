package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ExternalSystemAuthenticationEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public class ExternalSystemAuthenticationEntityMgrTestNG extends CDLFunctionalTestNGBase {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private ExternalSystemAuthenticationEntityMgr extSysAuthenticationEntityMgr;

    private LookupIdMap lookupIdMapRef;
    private ExternalSystemAuthentication extSysAuthRef;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
        List<LookupIdMap> lookupIdsMapping = lookupIdMappingEntityMgr.getLookupIdsMapping(null, null, true);
        assertNotNull(lookupIdsMapping);
        assertEquals(lookupIdsMapping.size(), 0, JsonUtils.serialize(lookupIdsMapping));
        List<ExternalSystemAuthentication> auths = extSysAuthenticationEntityMgr.findAuthentications();
        assertNotNull(auths);
        assertEquals(auths.size(), 0);
    }

    @Test(groups = "functional")
    public void testCreate() {
        log.info("Creating LookupID Mapping");
        lookupIdMapRef = new LookupIdMap();
        lookupIdMapRef.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMapRef.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMapRef.setOrgId(lookupIdMapRef.getExternalSystemName() + "_" + System.currentTimeMillis());
        lookupIdMapRef.setOrgName(lookupIdMapRef.getExternalSystemName() + "_Test");
        lookupIdMapRef = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMapRef);
        assertNotNull(lookupIdMapRef);
        assertNotNull(lookupIdMapRef.getId());

        log.info("Creating External System Authentication");
        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setLookupMapConfigId(lookupIdMapRef.getId());
        extSysAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        extSysAuth.setSolutionInstanceId(UUID.randomUUID().toString());
        extSysAuth = extSysAuthenticationEntityMgr.createAuthentication(extSysAuth);
        assertNotNull(extSysAuth);
        assertNotNull(extSysAuth.getTrayAuthenticationId());
        assertNotNull(extSysAuth.getSolutionInstanceId());
        assertNotNull(extSysAuth.getLookupIdMap());
        extSysAuthRef = extSysAuth;
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testFindAll() {
        List<ExternalSystemAuthentication> extSysAuthList = extSysAuthenticationEntityMgr.findAuthentications();
        assertNotNull(extSysAuthList);
        assertEquals(extSysAuthList.size(), 1);

        // Check whether it loaded the transient value LookupMapConfigId
        ExternalSystemAuthentication extSysAuth = extSysAuthList.get(0);
        verifyCurrentObject(extSysAuth);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testUpdate() {
        ExternalSystemAuthentication extSysAuth = extSysAuthenticationEntityMgr
                .findAuthenticationByAuthId(extSysAuthRef.getId());
        verifyCurrentObject(extSysAuth);
        log.info(JsonUtils.serialize(extSysAuth));
        ExternalSystemAuthentication updatedSysAuth = new ExternalSystemAuthentication();
        updatedSysAuth.setId(extSysAuth.getId());
        updatedSysAuth.setTrayAuthenticationId(UUID.randomUUID().toString());
        updatedSysAuth.setSolutionInstanceId(UUID.randomUUID().toString());
        extSysAuthenticationEntityMgr.updateAuthentication(extSysAuth.getId(), updatedSysAuth);
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            // Ignore
        }
        updatedSysAuth = extSysAuthenticationEntityMgr.findAuthenticationByAuthId(extSysAuthRef.getId());
        verifyCurrentObject(updatedSysAuth);
        log.info(JsonUtils.serialize(updatedSysAuth));
        assertNotEquals(updatedSysAuth.getTrayAuthenticationId(), extSysAuth.getTrayAuthenticationId());
        assertNotEquals(updatedSysAuth.getSolutionInstanceId(), extSysAuth.getSolutionInstanceId(),
                "SolutionInstaceID was not updated");
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testFindByLookupIdMap() {
        List<String> lookupMapIds = Collections.singletonList(lookupIdMapRef.getId());
        List<ExternalSystemAuthentication> extSysAuthList = extSysAuthenticationEntityMgr
                .findAuthenticationsByLookupMapIds(lookupMapIds);
        assertNotNull(extSysAuthList);
        assertEquals(extSysAuthList.size(), 1);

        // Check whether it loaded the transient value LookupMapConfigId
        ExternalSystemAuthentication extSysAuth = extSysAuthList.get(0);
        verifyCurrentObject(extSysAuth);
    }

    private void verifyCurrentObject(ExternalSystemAuthentication extSysAuth) {
        assertNotNull(extSysAuth);
        assertNotNull(extSysAuth.getId());
        assertNotNull(extSysAuth.getLookupMapConfigId());
        assertEquals(extSysAuth.getId(), extSysAuthRef.getId());
        assertEquals(extSysAuth.getLookupMapConfigId(), lookupIdMapRef.getId());
    }

}
