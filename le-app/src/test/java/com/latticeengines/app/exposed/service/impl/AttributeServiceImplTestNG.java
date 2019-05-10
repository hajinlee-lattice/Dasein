package com.latticeengines.app.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Arrays;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationPropertyEntityMgr;
import com.latticeengines.app.exposed.entitymanager.CategoryCustomizationPropertyEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.CategoryNameUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class AttributeServiceImplTestNG extends AppFunctionalTestNGBase {

    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace
            .parse(AttributeServiceImplTestNG.class.getSimpleName());
    private static final String ATTRIBUTE_NAME_1 = "TestAttribute_1";
    private static final String ATTRIBUTE_NAME_2 = "TestAttribute_2";
    private static final String ATTRIBUTE_NAME_3 = "TestAttribute_3";

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

        ColumnMetadata cm1 = new ColumnMetadata();
        cm1.setAttrName(ATTRIBUTE_NAME_1);
        cm1.setCategory(category1);
        cm1.setSubcategory(subcategory1);

        ColumnMetadata cm2 = new ColumnMetadata();
        cm2.setAttrName(ATTRIBUTE_NAME_2);
        cm2.setCategory(category1);
        cm2.setSubcategory(subcategory2);

        ColumnMetadata cm3 = new ColumnMetadata();
        cm3.setAttrName(ATTRIBUTE_NAME_3);
        cm3.setCategory(category2);
        cm3.setSubcategory(subcategory1);

        ColumnMetadataProxy proxy = Mockito.mock(ColumnMetadataProxy.class);
        Mockito.when(proxy.latestVersion(null)).thenReturn(new DataCloudVersion());
        Mockito.when(proxy.columnSelection(ColumnSelection.Predefined.Enrichment))
                .thenReturn(Arrays.asList(cm1, cm2, cm3));

        ReflectionTestUtils.setField(attributeService, "columnMetadataProxy", proxy);
    }

    @AfterClass(groups = "functional")
    public void cleanUp() {
        Tenant tenant = tenantService.findByTenantId(CUSTOMER_SPACE.toString());

        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
    }

    @Test(groups = "functional")
    public void saveAttributeProperty() {
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile, propertyName),
                Boolean.FALSE.toString());
        attributeCustomizationService.save(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile, propertyName,
                propertyValue);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_2, AttributeUseCase.CompanyProfile, propertyName),
                Boolean.FALSE.toString());

        attributeCustomizationService.save(ATTRIBUTE_NAME_3, AttributeUseCase.CompanyProfile, propertyName,
                propertyValue);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_3, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue);
    }

    @Test(groups = "functional", dependsOnMethods = "saveAttributeProperty")
    public void saveCategory() {
        attributeCustomizationService.saveCategory(category1, AttributeUseCase.CompanyProfile, propertyName,
                propertyValue2);
        String value = attributeCustomizationService.retrieve(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile,
                propertyName);
        assertEquals(value, propertyValue2);
        assertNull(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile,
                propertyName));
        assertEquals(
                categoryCustomizationPropertyEntityMgr
                        .find(AttributeUseCase.CompanyProfile, category1.getName(), propertyName).getPropertyValue(),
                propertyValue2);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_2, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue2);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_3, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue);
    }

    @Test(groups = "functional", dependsOnMethods = "saveCategory")
    public void saveSubCategory() {
        attributeCustomizationService.saveSubcategory(category1, subcategory1, AttributeUseCase.CompanyProfile,
                propertyName, propertyValue);
        String value = attributeCustomizationService.retrieve(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile,
                propertyName);
        assertEquals(value, propertyValue);
        assertNull(attributeCustomizationPropertyEntityMgr.find(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile,
                propertyName));
        assertEquals(
                categoryCustomizationPropertyEntityMgr
                        .find(AttributeUseCase.CompanyProfile, category1.getName(), propertyName).getPropertyValue(),
                propertyValue2);
        String categoryName = CategoryNameUtils.getCategoryName(category1.getName(), subcategory1);
        assertEquals(categoryCustomizationPropertyEntityMgr
                .find(AttributeUseCase.CompanyProfile, categoryName, propertyName).getPropertyValue(), propertyValue);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_2, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue2);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_3, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue);
    }

    @Test(groups = "functional", dependsOnMethods = "saveSubCategory")
    public void saveAttributeProperty2() {
        assertEquals(attributeCustomizationService.retrieve(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile,
                propertyName2), Boolean.FALSE.toString());
        attributeCustomizationService.save(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile, propertyName2,
                propertyValue);
        assertEquals(attributeCustomizationService.retrieve(ATTRIBUTE_NAME_1, AttributeUseCase.CompanyProfile,
                propertyName2), propertyValue);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_2, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue2);
        assertEquals(
                attributeCustomizationService.retrieve(ATTRIBUTE_NAME_3, AttributeUseCase.CompanyProfile, propertyName),
                propertyValue);
    }
}
