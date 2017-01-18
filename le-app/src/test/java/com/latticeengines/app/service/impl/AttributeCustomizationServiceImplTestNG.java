package com.latticeengines.app.service.impl;

import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.testframework.AppTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.attribute.AttributeCustomization;
import com.latticeengines.domain.exposed.attribute.AttributeUseCase;
import com.latticeengines.domain.exposed.attribute.CompanyProfileAttributeFlags;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class AttributeCustomizationServiceImplTestNG extends AppTestNGBase {
    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace.parse("AttributeCustomizationServiceImplTestNG");
    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @Autowired
    private AttributeCustomizationEntityMgr attributeCustomizationEntityMgr;

    @BeforeClass(groups = "functional")
    private void setUp() {
        createCompositeTable(attributeCustomizationEntityMgr.getRepository(),
                attributeCustomizationEntityMgr.getRecordType());
        Tenant tenant = new Tenant();
        tenant.setId(CustomerSpace.parse("AttributeCustomizationServiceImplTestNG").toString());
        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "functional")
    public void save() {
        CompanyProfileAttributeFlags flags = new CompanyProfileAttributeFlags();
        flags.setHidden(true);
        flags.setHighlighted(false);
        attributeCustomizationService.save("TestAttribute", AttributeUseCase.CompanyProfile, flags);
    }

    @Test(groups = "functional", dependsOnMethods = "save")
    public void retrieve() {
        AttributeCustomization customization = attributeCustomizationService.retrieve("TestAttribute");
        assertNotNull(customization);
    }
}
