package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class TableEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private MetadataService metadataService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        metadataService.deleteTable(CustomerSpace.parse(customerSpace2), TABLE2);
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace2));
        Table t = tableEntityMgr.findByName(TABLE2);
        assertNull(t);
    }

    @Test(groups = "functional")
    public void testCreateRetry() {
        try {
            Table table = createTable(tenantEntityMgr.findByTenantId(customerSpace2), TABLE2,
                    tableLocation2.append(TABLE2).toString());
            table.setDisplayName(null); // this will fail a NOT NULL constraint
            metadataService.createTable(CustomerSpace.parse(customerSpace1), table);
        } catch (Exception e) {
            Throwable inner = e.getCause();
            assertTrue(inner.getMessage().contains("DISPLAY_NAME"));
        }
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void findAll(String customerSpace, String tableName) {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));
        List<Table> tables = tableEntityMgr.findAll();

        assertEquals(tables.size(), 1);
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void findByName(String customerSpace, String tableName) {
        MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(customerSpace));

        Table table = tableEntityMgr.findByName(tableName);
        addDataRules(table);
        metadataService.updateTable(CustomerSpace.parse(customerSpace1), table);
        validateTable(table);

        Table retrievedTable = tableEntityMgr.findByName(table.getName());
        assertEquals(retrievedTable.getDataRules().size(), 3);
        String serializedStr = JsonUtils.serialize(retrievedTable);

        Table deserializedTable = JsonUtils.deserialize(serializedStr, Table.class);
        validateTable(deserializedTable);
    }

    private void addDataRules(Table table) {
        List<DataRule> dataRules = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            DataRule rule = new DataRule("rule" + i);
            rule.setTable(table);

            List<String> columnsToReview = new ArrayList<>();
            columnsToReview.add("Column" + i);
            rule.setFlaggedColumnNames(columnsToReview);
            rule.setDescription("desc");
            rule.setEnabled(true);
            rule.setMandatoryRemoval(false);
            Map<String, Object> properties = new HashMap<>();
            properties.put("customdomains", "a.com, b.com, c.com");
            rule.setProperties(properties);
            dataRules.add(rule);
        }
        table.setDataRules(dataRules);
    }

    private void validateTable(Table table) {
        List<Attribute> attrs = table.getAttributes();

        assertEquals(attrs.get(3).getApprovedUsage().get(0), "Model");
        assertEquals(attrs.get(3).getDataSource().get(0), "DerivedColumns");
        assertEquals(attrs.get(3).getCategory(), "Firmographics");
        assertEquals(attrs.get(3).getDataType(), "Int");
        assertEquals(attrs.get(3).getStatisticalType(), "ratio");
        assertEquals(attrs.get(3).getFundamentalType(), "numeric");
        assertEquals(attrs.get(3).getTags().get(0), "External");
        assertEquals(attrs.get(3).getSourceLogicalDataType(), "Integer");
        assertEquals(attrs.get(3).getApprovedUsage().get(0), "Model");
        assertEquals(attrs.get(3).getFundamentalType(), "numeric");
        assertEquals(attrs.get(3).getStatisticalType(), "ratio");
        assertEquals(attrs.get(3).getDataSource().get(0), "DerivedColumns");
    }

    @Test(groups = "functional")
    public void createTags() {

    }

    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] { {customerSpace1, TABLE1 }, {customerSpace2, TABLE1 }, };
    }

}
