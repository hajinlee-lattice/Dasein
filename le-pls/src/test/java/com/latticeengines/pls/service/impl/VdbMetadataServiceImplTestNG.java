package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.pls.functionalframework.VdbMetadataFieldFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.VdbMetadataConstants;
import com.latticeengines.pls.service.VdbMetadataService;

public class VdbMetadataServiceImplTestNG extends VdbMetadataFieldFunctionalTestNGBase {

    // TODO: need to implement mock up API
    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Autowired
    private VdbMetadataService vdbMetadataService;

    private static final String tags[] = { "Internal", "External" };
    private static final String categories[] = { "Lead Information", "Marketing Activity" };
    private static final String approvedUsages[] = { "None", "Model", "ModelAndAllInsights", "ModelAndModelInsights" };
    private static final String fundamentalTypes[] = { "boolean", "currency", "numeric", "percentage", "year" };
    private static final String statisticalTypes[] = { "interval", "nominal", "ordinal", "ratio" };

    private static final Integer maxUpdatesCount = 10;

    private Tenant tenant;
    private List<VdbMetadataField> originalFields;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupUsers();

        tenant = testingTenants.get(0);
        originalFields = vdbMetadataService.getFields(tenant);
    }

    @AfterClass(groups = { "functional" })
    public void teardown() throws Exception {
        if (originalFields != null && originalFields.size() > 0) {
            vdbMetadataService.UpdateFields(tenant, originalFields);
        }
    }

    @Test(groups = "functional" , enabled = false)
    public void getSourceToDisplay() {
        for (Map.Entry<String, String> entry : VdbMetadataConstants.SOURCE_MAPPING.entrySet()) {
            Assert.assertEquals(entry.getValue(), vdbMetadataService.getSourceToDisplay(entry.getKey()));
        }
    }

    @Test(groups = "functional" , enabled = false)
    public void testGetFields() throws Exception {
        List<VdbMetadataField> fields = vdbMetadataService.getFields(tenant);

        String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
        ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlUrl);
        Map<String, Map<String, String>> colsMetadata = connMgr.getMetadata(VdbMetadataConstants.MODELING_QUERY_NAME);
        for (VdbMetadataField field : fields) {
            assertFieldExists(colsMetadata, field);
        }
    }

    @Test(groups = "functional", dataProvider = "noAttributeChangedDataProviderArgs", enabled = false)
    public void testUpdateFieldWithNoAttributeChanged(VdbMetadataField field) {
        VdbMetadataField originalField = originalFields.get(0);
        vdbMetadataService.UpdateField(tenant, field);
        List<VdbMetadataField> fieldsUpdated = vdbMetadataService.getFields(tenant);
        VdbMetadataField fieldUpdated = getField(fieldsUpdated, field.getColumnName());
        Assert.assertTrue(originalField.equals(fieldUpdated));
    }

    @Test(groups = "functional", enabled = false)
    public void testUpdateFieldWithAllAttributesChanged() throws Exception {
        VdbMetadataField field = (VdbMetadataField)originalFields.get(0).clone();
        if ("DisplayName_FunTest".equals(field.getDisplayName()))
            field.setDisplayName("DisplayName_FunTest_A");
        else
            field.setDisplayName("DisplayName_FunTest");
        if ("Internal".equals(field.getTags()))
            field.setTags("External");
        else
            field.setTags("Internal");
        if ("Lead Information".equals(field.getCategory()))
            field.setCategory("Marketing Activity");
        else
            field.setCategory("Lead Information");
        if ("None".equals(field.getApprovedUsage()))
            field.setApprovedUsage("Model");
        else
            field.setApprovedUsage("None");
        if ("boolean".equals(field.getFundamentalType()))
            field.setFundamentalType("currency");
        else
            field.setFundamentalType("boolean");
        if ("Description_FunTest".equals(field.getDescription()))
            field.setDescription("Description_FunTest_A");
        else
            field.setDescription("Description_FunTest");
        if ("{\"geometric\":{\"minValue\":1}}".equals(field.getDisplayDiscretization()))
            field.setDisplayDiscretization("{\"geometric\":{\"minValue\":0}}");
        else
            field.setDisplayDiscretization("{\"geometric\":{\"minValue\":1}}");
        if ("interval".equals(field.getStatisticalType()))
            field.setStatisticalType("nominal");
        else
            field.setStatisticalType("interval");
        vdbMetadataService.UpdateField(tenant, field);
        List<VdbMetadataField> fieldsUpdated = vdbMetadataService.getFields(tenant);
        VdbMetadataField fieldUpdated = getField(fieldsUpdated, field.getColumnName());
        Assert.assertTrue(field.equals(fieldUpdated));

        // Assert field was also updated in custom query
        String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
        ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlUrl);
        Map<String, Map<String, String>> colsMetadataInCustomQuery = connMgr.getMetadata(VdbMetadataConstants.CUSTOM_QUERY_NAME);
        assertFieldExists(colsMetadataInCustomQuery, field);
    }

    @Test(groups = "functional", enabled = false)
    public void testUpdateFieldWithAllAttributeEnums() {
        List<VdbMetadataField> fieldsUpdated;
        VdbMetadataField fieldUpdated;
        VdbMetadataField field = (VdbMetadataField)originalFields.get(0).clone();

        // All values for Tag
        for (String tag : tags) {
            field.setTags(tag);
            vdbMetadataService.UpdateField(tenant, field);
            fieldsUpdated = vdbMetadataService.getFields(tenant);
            fieldUpdated = getField(fieldsUpdated, field.getColumnName());
            Assert.assertTrue(field.equals(fieldUpdated));
        }

        // All values for Category
        for (String category : categories) {
            field.setCategory(category);
            vdbMetadataService.UpdateField(tenant, field);
            fieldsUpdated = vdbMetadataService.getFields(tenant);
            fieldUpdated = getField(fieldsUpdated, field.getColumnName());
            Assert.assertTrue(field.equals(fieldUpdated));
        }

        // All values for ApprovedUsage
        for (String approvedUsage : approvedUsages) {
            field.setApprovedUsage(approvedUsage);
            vdbMetadataService.UpdateField(tenant, field);
            fieldsUpdated = vdbMetadataService.getFields(tenant);
            fieldUpdated = getField(fieldsUpdated, field.getColumnName());
            Assert.assertTrue(field.equals(fieldUpdated));
        }

        // All values for FundamentalType
        for (String fundamentalType : fundamentalTypes) {
            field.setFundamentalType(fundamentalType);
            vdbMetadataService.UpdateField(tenant, field);
            fieldsUpdated = vdbMetadataService.getFields(tenant);
            fieldUpdated = getField(fieldsUpdated, field.getColumnName());
            Assert.assertTrue(field.equals(fieldUpdated));
        }

        // All values for StatisticalType
        for (String statisticalType : statisticalTypes) {
            field.setStatisticalType(statisticalType);
            vdbMetadataService.UpdateField(tenant, field);
            fieldsUpdated = vdbMetadataService.getFields(tenant);
            fieldUpdated = getField(fieldsUpdated, field.getColumnName());
            Assert.assertTrue(field.equals(fieldUpdated));
        }
    }

    @Test(groups = "functional", enabled = false)
    public void testUpdateFields() throws Exception {
        List<VdbMetadataField> fieldsToUpdate = new ArrayList<VdbMetadataField>();
        Integer maxCount = originalFields.size() > maxUpdatesCount ? maxUpdatesCount : originalFields.size();
        for (int i = 0; i < maxCount; i++) {
            VdbMetadataField field = (VdbMetadataField)originalFields.get(i).clone();
            field.setDisplayName("DisplayName_FunTest_" + i);
            field.setTags(tags[i % tags.length]);
            field.setCategory(categories[i % categories.length]);
            field.setApprovedUsage(approvedUsages[i % approvedUsages.length]);
            field.setFundamentalType(fundamentalTypes[i % fundamentalTypes.length]);
            field.setDescription("Description_FunTest" + i);
            field.setDisplayDiscretization("{\"geometric\":{\"minValue\":" + i + "}}");
            field.setStatisticalType(statisticalTypes[i % statisticalTypes.length]);
            fieldsToUpdate.add(field);
        }
        vdbMetadataService.UpdateFields(tenant, fieldsToUpdate);

        List<VdbMetadataField> fieldsUpdated = vdbMetadataService.getFields(tenant);
        for (VdbMetadataField field : fieldsToUpdate) {
            VdbMetadataField fieldUpdated = getField(fieldsUpdated, field.getColumnName());
            Assert.assertTrue(field.equals(fieldUpdated));
        }

        // Assert fields were also updated in custom query
        String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
        String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
        ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr("visiDB", tenantName, dlUrl);
        Map<String, Map<String, String>> colsMetadataInCustomQuery = connMgr.getMetadata(VdbMetadataConstants.CUSTOM_QUERY_NAME);
        for (VdbMetadataField field : fieldsToUpdate) {
            assertFieldExists(colsMetadataInCustomQuery, field);
        }
    }

    @DataProvider(name = "noAttributeChangedDataProviderArgs")
    public Object[][] noAttributeChangedDataProviderArgs() {
        VdbMetadataField field = originalFields.get(0);
        VdbMetadataField field1 = (VdbMetadataField)field.clone();
        VdbMetadataField field2 = (VdbMetadataField)field.clone();
        field2.setTags(null);
        field2.setCategory(null);
        field2.setApprovedUsage(null);
        field2.setFundamentalType(null);
        field2.setStatisticalType(null);
        VdbMetadataField field3 = (VdbMetadataField)field.clone();
        field3.setTags("");
        field3.setCategory("");
        field3.setApprovedUsage("");
        field3.setFundamentalType("");
        field3.setStatisticalType("");

        return new Object[][] { { field1 }, //
                { field2 }, //
                { field3 } };
    }

}