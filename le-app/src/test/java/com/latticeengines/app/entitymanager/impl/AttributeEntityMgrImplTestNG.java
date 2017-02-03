package com.latticeengines.app.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.testframework.AppTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.AttributeCustomization;
import com.latticeengines.domain.exposed.pls.AttributeFlags;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CompanyProfileAttributeFlags;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class AttributeEntityMgrImplTestNG extends AppTestNGBase {
    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace.parse("AttributeEntityMgrImplTestNG");
    private static final String ATTRIBUTE_NAME = "TestAttribute";

    @Autowired
    private AttributeCustomizationEntityMgr attributeCustomizationEntityMgr;

    @Autowired
    private AttributeService attributeService;

    @Autowired
    private TenantService tenantService;
    private AttributeFlags saved;

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
        AttributeCustomization customization = new AttributeCustomization();
        customization.setName(ATTRIBUTE_NAME);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setFlags(new CompanyProfileAttributeFlags(true, false));
        saved = customization.getFlags();
        attributeCustomizationEntityMgr.createOrUpdate(customization);
    }

    @Test(groups = "functional", dependsOnMethods = "save")
    public void update() {
        AttributeCustomization customization = new AttributeCustomization();
        customization.setName(ATTRIBUTE_NAME);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setFlags(new CompanyProfileAttributeFlags(false, true));
        saved = customization.getFlags();
        attributeCustomizationEntityMgr.createOrUpdate(customization);
    }

    @Test(groups = "functional", dependsOnMethods = "update")
    public void retrieve() {
        AttributeCustomization customization = attributeCustomizationEntityMgr.find(ATTRIBUTE_NAME,
                AttributeUseCase.CompanyProfile);
        assertNotNull(customization);
        assertEquals(customization.getFlags(), saved);
    }
}
