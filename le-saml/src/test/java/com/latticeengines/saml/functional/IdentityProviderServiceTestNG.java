package com.latticeengines.saml.functional;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.domain.exposed.saml.SamlConfigMetadata;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;
import com.latticeengines.saml.service.IdentityProviderService;
import com.latticeengines.saml.testframework.SamlTestBed;
import com.latticeengines.saml.testframework.SamlTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
public class IdentityProviderServiceTestNG extends SamlTestNGBase {

    @Inject
    private IdentityProviderService identityProviderService;

    @Inject
    private IdentityProviderEntityMgr identityProviderEntityMgr;

    @Inject
    private TenantService tenantService;

    @Inject
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
    public void testFailValidation1() {
        IdentityProvider bad = samlFunctionalTestBed.constructIdp();
        bad.setMetadata("<bad xml>");
        identityProviderEntityMgr.create(bad);
    }

    @Test(groups = "functional", expectedExceptions = LedpException.class)
    public void testFailValidation2() {
        IdentityProvider bad = samlFunctionalTestBed.constructIdp();
        bad.setMetadata(null);
        identityProviderEntityMgr.create(bad);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testGetSamlConfigMetadata() {
        Tenant tenant = samlFunctionalTestBed.getGlobalAuthTestBed().getMainTestTenant();
        assertNotNull(tenant);
        SamlConfigMetadata samlConfigMetadata = identityProviderService.getConfigMetadata(tenant);
        assertNotNull(samlConfigMetadata);
        assertNotNull(samlConfigMetadata.getEntityId());
        assertNotNull(samlConfigMetadata.getSingleSignOnService());
    }
    
    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testExists() {
        IdentityProvider retrieved = identityProviderService.find(identityProvider.getEntityId());
        assertNotNull(retrieved);
        assertNotNull(retrieved.getEntityId());
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
        IdentityProvider retrieved = identityProviderEntityMgr.findByGATenantAndEntityId(null,
                identityProvider.getEntityId());
        Assert.assertNull(retrieved);
        tenantService.discardTenant(samlFunctionalTestBed.getGlobalAuthTestBed().getMainTestTenant());
        retrieved = identityProviderEntityMgr.findByGATenantAndEntityId(identityProvider.getGlobalAuthTenant(),
                identityProvider.getEntityId());
        assertNull(retrieved);
    }
}
