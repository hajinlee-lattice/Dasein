package com.latticeengines.app.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationPropertyEntityMgr;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.CategoryNameUtils;
import com.latticeengines.security.exposed.service.TenantService;

public class AttributeCustomizationPropertyEntityMgrImplTestNG extends AppFunctionalTestNGBase {
    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace
            .parse(AttributeCustomizationPropertyEntityMgrImplTestNG.class.getSimpleName());
    private static final String ATTRIBUTE_NAME_1 = "TestAttribute_1";
    private static final String ATTRIBUTE_NAME_2 = "TestAttribute_2";
    private static final String ATTRIBUTE_NAME_3 = "TestAttribute_3";

    @Inject
    private AttributeCustomizationPropertyEntityMgr attributeCustomizationPropertyEntityMgr;

    @Inject
    private TenantService tenantService;

    private String propertyName = "hidden";
    private String propertyValue = Boolean.TRUE.toString();
    private String propertyName2 = "highlighted";
    private String propertyValue2 = Boolean.FALSE.toString();
    private Category category1 = Category.DEFAULT;
    private Category category2 = Category.FIRMOGRAPHICS;
    private String subcategory1 = "a";
    private String subcategory2 = "b";

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

    @AfterClass(groups = "functional")
    public void cleanUp() {
        Tenant tenant = tenantService.findByTenantId(CUSTOMER_SPACE.toString());

        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }

    @Test(groups = "functional")
    public void saveProperty() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME_1);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(CategoryNameUtils.getCategoryName(category1.getName(), subcategory1));
        customization.setPropertyName(propertyName);
        customization.setPropertyValue(propertyValue);
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);

        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1).size(), 1);
        AttributeCustomizationProperty retrieved = attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1,
                AttributeUseCase.CompanyProfile, propertyName);
        assertNotNull(retrieved);
        assertEquals(retrieved.getPropertyValue(), propertyValue);
    }

    @Test(groups = "functional", dependsOnMethods = "saveProperty")
    public void updateProperty() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME_1);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(CategoryNameUtils.getCategoryName(category1.getName(), subcategory1));
        customization.setPropertyName(propertyName);
        customization.setPropertyValue(propertyValue2);
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);

        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1).size(), 1);
        AttributeCustomizationProperty retrieved = attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1,
                AttributeUseCase.CompanyProfile, propertyName);
        assertNotNull(retrieved);
        assertEquals(retrieved.getPropertyValue(), propertyValue2);

    }

    @Test(groups = "functional", dependsOnMethods = "updateProperty")
    public void saveAnotherProperty() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME_1);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(CategoryNameUtils.getCategoryName(category1.getName(), subcategory1));
        customization.setPropertyName(propertyName2);
        customization.setPropertyValue(propertyValue2);
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);

        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1).size(), 2);
        AttributeCustomizationProperty retrieved = attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1,
                AttributeUseCase.CompanyProfile, propertyName2);
        assertNotNull(retrieved);
        assertEquals(retrieved.getPropertyValue(), propertyValue2);
    }

    @Test(groups = "functional", dependsOnMethods = "saveAnotherProperty")
    public void savePropertyInDifferentSubcategory() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME_2);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(CategoryNameUtils.getCategoryName(category1.getName(), subcategory2));
        customization.setPropertyName(propertyName);
        customization.setPropertyValue(propertyValue);
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);

        AttributeCustomizationProperty retrieved = attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_2,
                AttributeUseCase.CompanyProfile, propertyName);
        assertNotNull(retrieved);
        assertEquals(retrieved.getPropertyValue(), propertyValue);

        customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME_2);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(CategoryNameUtils.getCategoryName(category1.getName(), subcategory2));
        customization.setPropertyName(propertyName2);
        customization.setPropertyValue(propertyValue2);
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);

        retrieved = attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_2, AttributeUseCase.CompanyProfile,
                propertyName2);
        assertNotNull(retrieved);
        assertEquals(retrieved.getPropertyValue(), propertyValue2);

        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_2).size(), 2);

    }

    @Test(groups = "functional", dependsOnMethods = "savePropertyInDifferentSubcategory")
    public void savePropertyInAnotherCategory() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME_3);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(CategoryNameUtils.getCategoryName(category2.getName(), subcategory1));
        customization.setPropertyName(propertyName2);
        customization.setPropertyValue(propertyValue2);
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);

        AttributeCustomizationProperty retrieved = attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_3,
                AttributeUseCase.CompanyProfile, propertyName2);
        assertNotNull(retrieved);
        assertEquals(retrieved.getPropertyValue(), propertyValue2);

        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_3).size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "savePropertyInAnotherCategory")
    public void delete() {
        assertEquals(attributeCustomizationPropertyEntityMgr.findAll().size(), 5);
        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1).size(), 2);
        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_2).size(), 2);
        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_3).size(), 1);

        String categoryName1 = CategoryNameUtils.getCategoryName(category1.getName(), subcategory1);
        attributeCustomizationPropertyEntityMgr.deleteSubcategory(categoryName1, AttributeUseCase.CompanyProfile,
                propertyName);
        assertEquals(attributeCustomizationPropertyEntityMgr.findAll().size(), 4);
        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1).size(), 1);

        attributeCustomizationPropertyEntityMgr.deleteCategory(category1, AttributeUseCase.CompanyProfile,
                propertyName2);
        assertEquals(attributeCustomizationPropertyEntityMgr.findAll().size(), 2);
        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1).size(), 0);
        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_2).size(), 1);

        attributeCustomizationPropertyEntityMgr.deleteCategory(category2, AttributeUseCase.CompanyProfile,
                propertyName2);
        assertEquals(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_3).size(), 0);
        assertEquals(attributeCustomizationPropertyEntityMgr.findAll().size(), 1);

    }

}
