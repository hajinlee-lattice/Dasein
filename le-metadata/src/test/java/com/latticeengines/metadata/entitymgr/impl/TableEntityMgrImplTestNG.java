package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.Comparator;
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
        
        Collections.sort(attrs, new Comparator<Attribute>() {

            @Override
            public int compare(Attribute o1, Attribute o2) {
                return o1.getName().compareTo(o2.getName());
            }
            
        });
        assertEquals(table.getAttributes().size(), 2);
        
        assertEquals(attrs.get(0).getName(), "ID");
        assertEquals(attrs.get(0).getDisplayName(), "Id");
        assertEquals(attrs.get(0).getLength().intValue(), 10);
        assertEquals(attrs.get(0).getPrecision().intValue(), 10);
        assertEquals(attrs.get(0).getScale().intValue(), 10);
        assertEquals(attrs.get(0).getPhysicalDataType(), "XYZ");
        assertEquals(attrs.get(0).getLogicalDataType(), "Identity");
        assertEquals(attrs.get(1).getName(), "LID");
        assertEquals(attrs.get(1).getDisplayName(), "LastUpdatedDate");
        assertEquals(attrs.get(1).getLength().intValue(), 20);
        assertEquals(attrs.get(1).getPrecision().intValue(), 20);
        assertEquals(attrs.get(1).getScale().intValue(), 20);
        assertEquals(attrs.get(1).getPhysicalDataType(), "ABC");
        assertEquals(attrs.get(1).getLogicalDataType(), "Date");
        assertEquals(table.getPrimaryKey().getAttributes().get(0), "ID");
        assertEquals(table.getLastModifiedKey().getAttributes().get(0), "LID");
    }
    
    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] {
                { CUSTOMERSPACE1, TABLE1 },
                { CUSTOMERSPACE2, TABLE2 },
        };
    }

}
