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
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.pls.service.VdbMetadataConstants;
import com.latticeengines.pls.service.VdbMetadataService;

public class VdbMetadataServiceImplDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Autowired
    private VdbMetadataService vdbMetadataService;

    private static final String[] categories = VdbMetadataConstants.CATEGORY_OPTIONS;
    private static final String[] approvedUsages = VdbMetadataConstants.APPROVED_USAGE_OPTIONS;
    private static final String[] fundamentalTypes = VdbMetadataConstants.FUNDAMENTAL_TYPE_OPTIONS;
    private static final String[] statisticalTypes = VdbMetadataConstants.STATISTICAL_TYPE_OPTIONS;

    private static final Integer maxUpdatesCount = 5;

    private Tenant tenant;
    private List<VdbMetadataField> originalFields;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        switchToSuperAdmin();

        tenant = testingTenants.get(0);
        originalFields = vdbMetadataService.getFields(tenant);
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() throws Exception {
        if (originalFields != null && originalFields.size() > 0) {
            if (originalFields.size() > maxUpdatesCount) {
                vdbMetadataService.UpdateFields(tenant, originalFields.subList(0, maxUpdatesCount));
            } else {
                vdbMetadataService.UpdateFields(tenant, originalFields);
            }
        }
    }

    @Test(groups = "deployment" , enabled = false)
    public void getSourceToDisplay() {
        for (Map.Entry<String, String> entry : VdbMetadataConstants.SOURCE_MAPPING.entrySet()) {
            Assert.assertEquals(entry.getValue(), vdbMetadataService.getSourceToDisplay(entry.getKey()));
        }
    }

    @Test(groups = "deployment" , enabled = false)
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

    @Test(groups = "deployment", dataProvider = "noAttributeChangedDataProviderArgs", enabled = false)
    public void testUpdateFieldWithNoAttributeChanged(VdbMetadataField field) {
        VdbMetadataField originalField = originalFields.get(0);
        vdbMetadataService.UpdateField(tenant, field);
        List<VdbMetadataField> fieldsUpdated = vdbMetadataService.getFields(tenant);
        VdbMetadataField fieldUpdated = getField(fieldsUpdated, field.getColumnName());
        Assert.assertTrue(originalField.equals(fieldUpdated));
    }

    @Test(groups = "deployment", enabled = false)
    public void testUpdateFieldWithAllAttributesChanged() throws Exception {
        VdbMetadataField field = (VdbMetadataField)originalFields.get(0).clone();
        if ("DisplayName_FunTest".equals(field.getDisplayName()))
            field.setDisplayName("DisplayName_FunTest_A");
        else
            field.setDisplayName("DisplayName_FunTest");
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

    @Test(groups = "deployment", enabled = false)
    public void testUpdateFieldWithAllAttributeEnums() {
        List<VdbMetadataField> fieldsUpdated;
        VdbMetadataField fieldUpdated;
        VdbMetadataField field = (VdbMetadataField)originalFields.get(0).clone();

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

    @Test(groups = "deployment", enabled = false)
    public void testUpdateFields() throws Exception {
        List<VdbMetadataField> fieldsToUpdate = new ArrayList<VdbMetadataField>();
        Integer maxCount = originalFields.size() > maxUpdatesCount ? maxUpdatesCount : originalFields.size();
        for (int i = 0; i < maxCount; i++) {
            VdbMetadataField field = (VdbMetadataField)originalFields.get(i).clone();
            field.setDisplayName("DisplayName_FunTest_" + i);
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

    private VdbMetadataField getField(List<VdbMetadataField> fields, String columnName) {
        for (VdbMetadataField field : fields) {
            if (columnName.equals(field.getColumnName())) {
                return field;
            }
        }

        return null;
    }

    private void assertFieldExists(Map<String, Map<String, String>> colsMetadata, VdbMetadataField field) {
        Assert.assertTrue(colsMetadata.containsKey(field.getColumnName()));
        Map<String, String> map = colsMetadata.get(field.getColumnName());
        Assert.assertEquals(field.getSource(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_SOURCE));
        Assert.assertEquals(field.getCategory(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_CATEGORY));
        Assert.assertEquals(field.getDisplayName(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_DISPLAYNAME));
        Assert.assertEquals(field.getDescription(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_DESCRIPTION));
        Assert.assertEquals(field.getApprovedUsage(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_APPROVED_USAGE));
        Assert.assertEquals(field.getTags(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_TAGS));
        Assert.assertEquals(field.getFundamentalType(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_TYPE));
        Assert.assertEquals(field.getDisplayDiscretization(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_DISPLAY_DISCRETIZATION));
        Assert.assertEquals(field.getStatisticalType(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_STATISTICAL_TYPE));
    }

    private String getFieldValue(Map<String, String> map, String key) {
        if (map.containsKey(key)) {
            String value = map.get(key);
            if (VdbMetadataConstants.ATTRIBUTE_NULL_VALUE.equalsIgnoreCase(value)) {
                return null;
            } else {
                return value;
            }
        } else {
            return null;
        }
    }

}