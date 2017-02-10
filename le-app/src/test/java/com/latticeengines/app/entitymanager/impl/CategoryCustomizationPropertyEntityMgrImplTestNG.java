package com.latticeengines.app.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationPropertyEntityMgr;
import com.latticeengines.app.exposed.entitymanager.CategoryCustomizationPropertyEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.testframework.AppTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class CategoryCustomizationPropertyEntityMgrImplTestNG extends AppTestNGBase {

    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace
            .parse(CategoryCustomizationPropertyEntityMgrImplTestNG.class.getSimpleName());
    private static final String ATTRIBUTE_NAME = "TestAttribute";

    @Autowired
    private AttributeCustomizationPropertyEntityMgr attributeCustomizationPropertyEntityMgr;

    @Autowired
    private CategoryCustomizationPropertyEntityMgr categoryCustomizationPropertyEntityMgr;

    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @Autowired
    private AttributeService attributeService;

    @Autowired
    private TenantService tenantService;

    private String propertyName = "hidden";
    private String propertyValue = "true";
    private String propertyName2 = "highlighted";
    private String propertyValue2 = "false";
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

        List<ColumnMetadata> list = new ArrayList<>();
        ColumnMetadata cm = new ColumnMetadata();
        cm.setColumnId(ATTRIBUTE_NAME);
        cm.setCategoryByString(category);
        cm.setSubcategory(subCategory);
        list.add(cm);

        ColumnMetadataProxy proxy = Mockito.mock(ColumnMetadataProxy.class);
        Mockito.when(proxy.latestVersion(null)).thenReturn(new DataCloudVersion());
        Mockito.when(proxy.columnSelection(ColumnSelection.Predefined.Enrichment, null)).thenReturn(list);

        ReflectionTestUtils.setField(attributeService, "columnMetadataProxy", proxy);
    }

    @Test(groups = "functional")
    public void saveCategory() {
        CategoryCustomizationProperty categoryCustomization = new CategoryCustomizationProperty();
        categoryCustomization.setCategoryName(category);
        categoryCustomization.setUseCase(AttributeUseCase.CompanyProfile);
        categoryCustomization.setPropertyName(propertyName);
        categoryCustomization.setPropertyValue(propertyValue);
        categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomization);
        saved = propertyValue;
        retrieve(propertyName);
    }

    @Test(groups = "functional", dependsOnMethods = "saveCategory")
    public void saveAttribute() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(String.format("%s.%s", category, subCategory));
        customization.setPropertyName(propertyName);
        customization.setPropertyValue(propertyValue2);
        saved = propertyValue2;
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);
        retrieve(propertyName);
        attributeCustomizationPropertyEntityMgr.delete(customization);
    }

    @Test(groups = "functional", dependsOnMethods = "saveAttribute")
    public void saveSubCategory() {
        CategoryCustomizationProperty categoryCustomization = new CategoryCustomizationProperty();
        categoryCustomization.setCategoryName(String.format("%s.%s", category, subCategory));
        categoryCustomization.setUseCase(AttributeUseCase.CompanyProfile);
        categoryCustomization.setPropertyName(propertyName);
        categoryCustomization.setPropertyValue(propertyValue);
        categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomization);
        saved = propertyValue;
        retrieve(propertyName);
    }

    @Test(groups = "functional", dependsOnMethods = "saveSubCategory")
    public void saveAttribute2() {
        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(ATTRIBUTE_NAME);
        customization.setUseCase(AttributeUseCase.CompanyProfile);
        customization.setCategoryName(String.format("%s.%s", category, subCategory));
        customization.setPropertyName(propertyName2);
        customization.setPropertyValue(propertyValue2);
        attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);
        retrieve(propertyName);
        saved = propertyValue2;
        retrieve(propertyName2);
    }

    private void retrieve(String propertyName) {
        String retrieved = attributeCustomizationService.retrieve(ATTRIBUTE_NAME, AttributeUseCase.CompanyProfile,
                propertyName);
        assertNotNull(retrieved);
        assertEquals(retrieved, saved);
    }
}
