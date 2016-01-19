package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.impl.SetTenantAspect;

public class TableEntityMgrImplTestNG extends MetadataFunctionalTestNGBase {

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void findAll(String customerSpace, String tableName) {
        new SetTenantAspect().setSecurityContext( //
                tenantEntityMgr.findByTenantId(customerSpace));
        List<Table> tables = tableEntityMgr.findAll();

        assertEquals(tables.size(), 1);
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void findByName(String customerSpace, String tableName) {
        new SetTenantAspect().setSecurityContext( //
                tenantEntityMgr.findByTenantId(customerSpace));

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
        assertEquals(attrs.get(3).getLogicalDataType(), "Integer");
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
