package com.latticeengines.app.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationPropertyEntityMgr;
import com.latticeengines.app.testframework.AppTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class AttributeCustomizationPropertyEntityMgrImplTestNG extends AppTestNGBase {
    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace.parse("AttributeEntityMgrImplTestNG");
    private static final String ATTRIBUTE_NAME = "TestAttribute";

    @Autowired
    private AttributeCustomizationPropertyEntityMgr attributeCustomizationPropertyEntityMgr;

    @Autowired
    private TenantService tenantService;

    private String propertyName = "hidden";
    private String propertyValue = "true";
    private String propertyName2 = "highlighted";
    private String category = "Firmographics";
    private String subCategory = "b";

    private String saved;

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
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(String.format("%s.%s", category, subCategory));
        customization.setPropertyName(propertyName);
        customization.setPropertyValue(propertyValue);
        saved = propertyValue;
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);
    }

    @Test(groups = "functional", dependsOnMethods = "save")
    public void update() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(String.format("%s.%s", category, subCategory));
        customization.setPropertyName(propertyName2);
        customization.setPropertyValue(propertyValue);
        saved = propertyValue;
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);
    }

    @Test(groups = "functional", dependsOnMethods = "update")
    public void retrieve() {
        AttributeCustomizationProperty customization = attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME,
                AttributeUseCase.CompanyProfile, propertyName2);
        assertNotNull(customization);
        assertEquals(customization.getPropertyValue(), saved);
    }
}
