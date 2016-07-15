package com.latticeengines.saml.functional;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.latticeengines.saml.testframework.SamlTestBed;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.saml.testframework.SamlTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

public class IdentityProviderServiceTestNG extends SamlTestNGBase {

    @Autowired
    private IdentityProviderService identityProviderService;

    @Autowired
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private SamlTestBed samlFunctionalTestBed;

    private IdentityProvider identityProvider;

    @BeforeClass(groups = "functional")
    public void setup() {
        samlFunctionalTestBed.setupTenant();
    }

    @Test(groups = "functional")
    public void testCreate() {
        identityProvider = samlFunctionalTestBed.constructIdp();
        identityProviderService.create(identityProvider);
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testFailValidation() {
        IdentityProvider bad = samlFunctionalTestBed.constructIdp();
        bad.setEntityId("bad entity id");
        identityProviderEntityMgr.create(bad);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testExists() {
        IdentityProvider retrieved = identityProviderService.find(identityProvider.getEntityId());
        assertNotNull(retrieved);
    }

    @Test(groups = "functional", dependsOnMethods = "testExists")
    public void testDuplicateEntityIdNotAllowed() {
        boolean thrown = false;
        try {
            identityProviderService.create(identityProvider);
        } catch (LedpException e) {
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test(groups = "functional", dependsOnMethods = "testDuplicateEntityIdNotAllowed")
    public void testDelete() {
        identityProviderService.delete(identityProvider.getEntityId());
    }

    @Test(groups = "functional", dependsOnMethods = "testDelete")
    public void testCreateAgain() {
        testCreate();
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateAgain")
    public void testCascadeDelete() {
        tenantService.discardTenant(samlFunctionalTestBed.getGlobalAuthTestBed().getMainTestTenant());
        IdentityProvider retrieved = identityProviderEntityMgr.findByEntityId(identityProvider.getEntityId());
        assertNull(retrieved);
    }
}
