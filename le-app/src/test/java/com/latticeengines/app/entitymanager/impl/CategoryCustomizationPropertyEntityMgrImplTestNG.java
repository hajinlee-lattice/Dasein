package com.latticeengines.app.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.CategoryCustomizationPropertyEntityMgr;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.CategoryNameUtils;
import com.latticeengines.security.exposed.service.TenantService;

public class CategoryCustomizationPropertyEntityMgrImplTestNG extends AppFunctionalTestNGBase {

    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace
            .parse(CategoryCustomizationPropertyEntityMgrImplTestNG.class.getSimpleName());

    @Inject
    private CategoryCustomizationPropertyEntityMgr categoryCustomizationPropertyEntityMgr;

    @Inject
    private TenantService tenantService;

    private String propertyName = "hidden";
    private String propertyValue = Boolean.TRUE.toString();
    private String propertyName2 = "highlighted";
    private String propertyValue2 = Boolean.FALSE.toString();
    private Category category = Category.FIRMOGRAPHICS;
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
    public void saveCategory1() {
        CategoryCustomizationProperty categoryCustomization = new CategoryCustomizationProperty();
        categoryCustomization.setCategoryName(category.getName());
        categoryCustomization.setUseCase(AttributeUseCase.CompanyProfile);
        categoryCustomization.setPropertyName(propertyName);
        categoryCustomization.setPropertyValue(propertyValue);
        categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomization);

        assertEquals(categoryCustomizationPropertyEntityMgr.findAll().size(), 1);
        CategoryCustomizationProperty retrieved = categoryCustomizationPropertyEntityMgr
                .find(AttributeUseCase.CompanyProfile, category.getName(), propertyName);
        assertEquals(categoryCustomization.getPropertyValue(), retrieved.getPropertyValue());
    }

    @Test(groups = "functional", dependsOnMethods = "saveCategory1")
    public void saveSubcategory1() {
        String categoryName = CategoryNameUtils.getCategoryName(category.getName(), subcategory1);
        CategoryCustomizationProperty categoryCustomization = new CategoryCustomizationProperty();
        categoryCustomization.setCategoryName(CategoryNameUtils.getCategoryName(category.getName(), subcategory1));
        categoryCustomization.setUseCase(AttributeUseCase.CompanyProfile);
        categoryCustomization.setPropertyName(propertyName);
        categoryCustomization.setPropertyValue(propertyValue2);
        categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomization);

        assertEquals(categoryCustomizationPropertyEntityMgr.findAll().size(), 2);
        CategoryCustomizationProperty retrieved = categoryCustomizationPropertyEntityMgr
                .find(AttributeUseCase.CompanyProfile, categoryName, propertyName);
        assertEquals(categoryCustomization.getPropertyValue(), retrieved.getPropertyValue());
    }

    @Test(groups = "functional", dependsOnMethods = "saveSubcategory1")
    public void saveCategory2() {
        CategoryCustomizationProperty categoryCustomization = new CategoryCustomizationProperty();
        categoryCustomization.setCategoryName(category.getName());
        categoryCustomization.setUseCase(AttributeUseCase.CompanyProfile);
        categoryCustomization.setPropertyName(propertyName2);
        categoryCustomization.setPropertyValue(propertyValue2);
        categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomization);

        assertEquals(categoryCustomizationPropertyEntityMgr.findAll().size(), 3);
        CategoryCustomizationProperty retrieved = categoryCustomizationPropertyEntityMgr
                .find(AttributeUseCase.CompanyProfile, category.getName(), propertyName2);
        assertEquals(categoryCustomization.getPropertyValue(), retrieved.getPropertyValue());
    }

    @Test(groups = "functional", dependsOnMethods = "saveCategory2")
    public void saveSubCategory2() {
        String categoryName = CategoryNameUtils.getCategoryName(category.getName(), subcategory1);
        CategoryCustomizationProperty categoryCustomization = new CategoryCustomizationProperty();
        categoryCustomization.setCategoryName(categoryName);
        categoryCustomization.setUseCase(AttributeUseCase.CompanyProfile);
        categoryCustomization.setPropertyName(propertyName2);
        categoryCustomization.setPropertyValue(propertyValue);
        categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomization);

        assertEquals(categoryCustomizationPropertyEntityMgr.findAll().size(), 4);
        CategoryCustomizationProperty retrieved = categoryCustomizationPropertyEntityMgr
                .find(AttributeUseCase.CompanyProfile, categoryName, propertyName2);
        assertEquals(categoryCustomization.getPropertyValue(), retrieved.getPropertyValue());
    }

    @Test(groups = "functional", dependsOnMethods = "saveSubCategory2")
    public void saveSubCategory3() {
        String categoryName = CategoryNameUtils.getCategoryName(category.getName(), subcategory2);
        CategoryCustomizationProperty categoryCustomization = new CategoryCustomizationProperty();
        categoryCustomization.setCategoryName(categoryName);
        categoryCustomization.setUseCase(AttributeUseCase.CompanyProfile);
        categoryCustomization.setPropertyName(propertyName);
        categoryCustomization.setPropertyValue(propertyValue);
        categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomization);

        assertEquals(categoryCustomizationPropertyEntityMgr.findAll().size(), 5);
        CategoryCustomizationProperty retrieved = categoryCustomizationPropertyEntityMgr
                .find(AttributeUseCase.CompanyProfile, categoryName, propertyName);
        assertEquals(categoryCustomization.getPropertyValue(), retrieved.getPropertyValue());
    }

    @Test(groups = "functional", dependsOnMethods = "saveSubCategory3")
    public void deleteCategory() {
        categoryCustomizationPropertyEntityMgr.deleteSubcategories(category, AttributeUseCase.CompanyProfile,
                propertyName);
        List<CategoryCustomizationProperty> retrieved = categoryCustomizationPropertyEntityMgr.findAll();
        assertEquals(retrieved.size(), 3);
        System.out.println(retrieved);
        assertEquals(retrieved.stream() //
                .filter(a -> a.getCategoryName().equals(category.getName()) && //
                        a.getPropertyName().equals(propertyName))
                .count(), 1);

        assertEquals(retrieved.stream() //
                .filter(a -> a.getPropertyName().equals(propertyName2)).count(), 2);

    }
}
