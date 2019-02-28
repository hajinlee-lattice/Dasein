package com.latticeengines.apps.lp.service.impl;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.apps.lp.testframework.LPFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class PythonScriptModelServiceTestNG extends LPFunctionalTestNGBase {

    private static final String TENANT1 = "TENANT1";
    private static final Attribute ATTRIBUTE_1 = new Attribute();
    private static final Attribute ATTRIBUTE_2 = new Attribute();
    private static final Attribute ATTRIBUTE_3 = new Attribute();
    private static final Table TABLE = new Table();
    private static final String ATTRIBUTE_NAME_ID = "Id";
    private static final String ATTRIBUTE_NAME_1 = "Name_1";
    private static final String ATTRIBUTE_NAME_2 = "Name_2";
    private static final ModelSummary MODEL_SUMMARY = new ModelSummary();
    private static final String MODEL_ID = "MODEL_ID";
    private static final String TABLE_NAME = "TABLE_NAME";

    @Autowired
    private PythonScriptModelService pythonScriptModelService;

    @Autowired
    private TenantService tenantService;

    private ModelSummaryService mockedModelSummaryService = Mockito.mock(ModelSummaryService.class);
    private MetadataProxy mockedMetadataProxy = Mockito.mock(MetadataProxy.class);

    private void setupTenant(String t) {
        Tenant tenant = tenantService.findByTenantId(t);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(t);
        tenant.setName(t);
        tenantService.registerTenant(tenant);

        MultiTenantContext.setTenant(tenant);
    }

    @BeforeClass(groups = { "functional" })
    public void setup() {
        setupTenant(TENANT1);
        ReflectionTestUtils.setField(pythonScriptModelService, "modelSummaryService",
                mockedModelSummaryService);
        ReflectionTestUtils.setField(pythonScriptModelService, "metadataProxy",
                mockedMetadataProxy);
        MODEL_SUMMARY.setEventTableName(TABLE_NAME);
        MODEL_SUMMARY.setSourceSchemaInterpretation(SchemaInterpretation.SalesforceLead.toString());

        ATTRIBUTE_1.setName(ATTRIBUTE_NAME_1);
        ATTRIBUTE_1.setTags(Tag.INTERNAL.toString());
        ATTRIBUTE_1.setApprovedUsage(ApprovedUsage.MODEL);
        ATTRIBUTE_2.setName(ATTRIBUTE_NAME_2);
        ATTRIBUTE_2.setTags(Tag.EXTERNAL.toString());
        ATTRIBUTE_3.setName(ATTRIBUTE_NAME_ID);
        ATTRIBUTE_3.setInterfaceName(ATTRIBUTE_NAME_ID);
        ATTRIBUTE_3.setTags(Tag.INTERNAL.toString());
        TABLE.addAttribute(ATTRIBUTE_1);
        TABLE.addAttribute(ATTRIBUTE_2);
        TABLE.addAttribute(ATTRIBUTE_3);

        when(mockedModelSummaryService.findValidByModelId(MODEL_ID)).thenReturn(MODEL_SUMMARY);
        when(mockedModelSummaryService.getModelSummaryByModelId(MODEL_ID)).thenReturn(MODEL_SUMMARY);
        when(mockedMetadataProxy.getTable(anyString(), eq(TABLE_NAME))).thenReturn(TABLE);
    }

    @Test(groups = "functional")
    public void getRequiredColumns_assertCorrectFieldsReturned() {
        List<Attribute> attributes = pythonScriptModelService.getRequiredColumns(MODEL_ID);

        assertEquals(attributes.size(), 2);
        Attribute foundAttribute = null;
        for (Attribute attribute : attributes) {
            if (attribute.getName().equals(ATTRIBUTE_NAME_1)) {
                foundAttribute = attribute;
            }
        }
        assertNotNull(foundAttribute);
        assertEquals(foundAttribute.getName(), ATTRIBUTE_NAME_1);
        assertEquals(foundAttribute.getTags().get(0), Tag.INTERNAL.toString());

        foundAttribute = null;
        for (Attribute attribute : attributes) {
            if (attribute.getName().equals(ATTRIBUTE_NAME_ID)) {
                foundAttribute = attribute;
            }
        }
        assertNotNull(foundAttribute);
        assertEquals(foundAttribute.getName(), ATTRIBUTE_NAME_ID);
    }

    @Test(groups = "functional")
    public void getLatticeAttributeNames_assertCorrectFieldsReturned() {
        Set<String> latticeAttributes = pythonScriptModelService.getLatticeAttributeNames(MODEL_ID);

        assertEquals(latticeAttributes.size(), 1);
        assertTrue(latticeAttributes.contains(ATTRIBUTE_NAME_2));
    }
}
