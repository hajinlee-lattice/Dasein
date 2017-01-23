package com.latticeengines.app.service.impl;

import static org.testng.Assert.assertEquals;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.testframework.AppTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.AttributeFlags;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CompanyProfileAttributeFlags;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class AttributeCustomizationServiceImplTestNG extends AppTestNGBase {
    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace.parse("AttributeCustomizationServiceImplTestNG");
    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @Autowired
    private AttributeCustomizationEntityMgr attributeCustomizationEntityMgr;

    @Autowired
    private TenantService tenantService;
    private CompanyProfileAttributeFlags saved;

    @BeforeClass(groups = "functional")
    private void setUp() {
        Tenant tenant = tenantService.findByTenantId(CUSTOMER_SPACE.toString());

        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(CUSTOMER_SPACE.toString());
        tenant.setName(CUSTOMER_SPACE.toString());

        globalAuthFunctionalTestBed.createTenant(tenant);
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "functional")
    public void save() {
        saved = new CompanyProfileAttributeFlags(true, false);
        attributeCustomizationService.save("TestAttribute", AttributeUseCase.CompanyProfile, saved);
    }

    @Test(groups = "functional", dependsOnMethods = "save")
    public void retrieve() {
        AttributeFlags flags = attributeCustomizationService.retrieve("TestAttribute", AttributeUseCase.CompanyProfile);
        assertEquals(flags, saved);
    }
}
