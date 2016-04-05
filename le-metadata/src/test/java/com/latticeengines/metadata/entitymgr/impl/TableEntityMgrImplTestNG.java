package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class TableEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private MetadataService metadataService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void testCreateRetry() {
        try {
            Table table = createTable(tenantEntityMgr.findByTenantId(CUSTOMERSPACE2), TABLE2,
                    tableLocation2.append(TABLE2).toString());
            table.setDisplayName(null); // this will fail a NOT NULL constraint
            metadataService.createTable(CustomerSpace.parse(CUSTOMERSPACE1), table);
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
        validateTable(table);

        String serializedStr = JsonUtils.serialize(table);

        Table deserializedTable = JsonUtils.deserialize(serializedStr, Table.class);
        validateTable(deserializedTable);
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

    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] { { CUSTOMERSPACE1, TABLE1 }, { CUSTOMERSPACE2, TABLE1 }, };
    }

}
