package com.latticeengines.saml.functional;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
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

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTenant();
    }

    @Test(groups = "functional")
    public void testCreate() {
        IdentityProvider identityProvider = new IdentityProvider();
        identityProvider.setEntityId("entityId");
        try {
            identityProvider.setMetadata(IOUtils.toString(getClass().getResourceAsStream(
                    RESOURCE_BASE + "/idp_metadata.xml")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        identityProviderService.create(identityProvider);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testExists() {
        IdentityProvider identityProvider = identityProviderService.find("entityId");
        assertNotNull(identityProvider);
    }

    @Test(groups = "functional", dependsOnMethods = "testExists")
    public void testDuplicateEntityIdNotAllowed() {
        boolean thrown = false;
        try {
            testCreate();
        } catch (LedpException e) {
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test(groups = "functional", dependsOnMethods = "testDuplicateEntityIdNotAllowed")
    public void testDelete() {
        identityProviderService.delete("entityId");
    }

    @Test(groups = "functional", dependsOnMethods = "testDelete")
    public void testCreateAgain() {
        testCreate();
    }

    @Test(groups = "functional", dependsOnMethods = "testCreateAgain")
    public void testCascadeDelete() {
        tenantService.discardTenant(globalAuthFunctionalTestBed.getMainTestTenant());
        IdentityProvider identityProvider = identityProviderEntityMgr.findByEntityId("entityId");
        assertNull(identityProvider);
    }
}
