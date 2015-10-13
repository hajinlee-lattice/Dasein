package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
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
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void validateTable(Table table) {
        List<Attribute> attrs = table.getAttributes();
        
        Collections.sort(attrs, new Comparator<Attribute>() {

            @Override
            public int compare(Attribute o1, Attribute o2) {
                return o1.getName().compareTo(o2.getName());
            }
            
        });
        assertEquals(table.getAttributes().size(), 4);
        
        assertEquals(attrs.get(0).getName(), "ActiveRetirementParticipants");
        assertEquals(attrs.get(0).getDisplayName(), "Active Retirement Plan Participants");
        assertEquals(attrs.get(0).getLength().intValue(), 5);
        assertEquals(attrs.get(0).getPrecision().intValue(), 0);
        assertEquals(attrs.get(0).getScale().intValue(), 0);
        assertEquals(attrs.get(0).getPhysicalDataType(), Schema.Type.INT.toString());
        assertEquals(attrs.get(0).getLogicalDataType(), "Integer");
        List<String> approvedUsageList = (List) attrs.get(0).getPropertyValue("ApprovedUsage");
        assertEquals(approvedUsageList.size(), 1);
        assertEquals(approvedUsageList.get(0), "Model");
        List kvList = (List) attrs.get(0).getPropertyValue("Extensions");
        assertEquals(kvList.size(), 2);
        
        if (kvList.get(0) instanceof AbstractMap.SimpleEntry) {
            List<Map.Entry<String, String>> kvListTypeKV = kvList;
            Collections.sort(kvListTypeKV, new Comparator<Map.Entry<String, String>>() {
                @Override
                public int compare(Entry<String, String> o1, Entry<String, String> o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
                
            });
            assertEquals(kvListTypeKV.get(0).getKey(), "Category");
            assertEquals(kvListTypeKV.get(0).getValue(), "Firmographics");
            assertEquals(kvListTypeKV.get(1).getKey(), "DataType");
            assertEquals(kvListTypeKV.get(1).getValue(), "Int");
        } else if (kvList.get(0) instanceof Map) {
            List<Map> kvListTypeMap = kvList;
            Collections.sort(kvListTypeMap, new Comparator<Map>() {

                @Override
                public int compare(Map o1, Map o2) {
                    return ((String) o1.get("key")).compareTo((String) o2.get("key"));
                }
                
            }); 
            assertEquals(kvListTypeMap.get(0).get("key"), "Category");
            assertEquals(kvListTypeMap.get(0).get("value"), "Firmographics");
            assertEquals(kvListTypeMap.get(1).get("key"), "DataType");
            assertEquals(kvListTypeMap.get(1).get("value"), "Int");
        }
        
        assertEquals((String) attrs.get(0).getPropertyValue("FundamentalType"), "numeric");
        assertEquals((String) attrs.get(0).getPropertyValue("StatisticalType"), "ratio");
        List<String> dataSources = (List) attrs.get(0).getPropertyValue("DataSource");
        assertEquals(dataSources.size(), 1);
        assertEquals(dataSources.get(0), "DerivedColumns");

        
        assertEquals(attrs.get(1).getName(), "ID");
        assertEquals(attrs.get(1).getDisplayName(), "Id");
        assertEquals(attrs.get(1).getLength().intValue(), 10);
        assertEquals(attrs.get(1).getPrecision().intValue(), 10);
        assertEquals(attrs.get(1).getScale().intValue(), 10);
        assertEquals(attrs.get(1).getPhysicalDataType(), "XYZ");
        assertEquals(attrs.get(1).getLogicalDataType(), "Identity");
        assertEquals(attrs.get(2).getName(), "LID");
        assertEquals(attrs.get(2).getDisplayName(), "LastUpdatedDate");
        assertEquals(attrs.get(2).getLength().intValue(), 20);
        assertEquals(attrs.get(2).getPrecision().intValue(), 20);
        assertEquals(attrs.get(2).getScale().intValue(), 20);
        assertEquals(attrs.get(2).getPhysicalDataType(), "ABC");
        assertEquals(attrs.get(2).getLogicalDataType(), "Date");
        assertEquals(attrs.get(3).getName(), "SPAM_INDICATOR");
        approvedUsageList = (List) attrs.get(3).getPropertyValue("ApprovedUsage");
        assertEquals(approvedUsageList.size(), 1);
        assertEquals(approvedUsageList.get(0), "ModelAndAllInsights");

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
