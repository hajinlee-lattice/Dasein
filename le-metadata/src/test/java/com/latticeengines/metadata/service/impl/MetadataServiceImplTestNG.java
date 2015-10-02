package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.AttributeOwnerType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;

public class MetadataServiceImplTestNG extends MetadataFunctionalTestNGBase {
    
    @Autowired
    private MetadataService mdService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void getTable(String customerSpace, String tableName) {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace), tableName);
        assertNotNull(table);
        assertEquals(table.getName(), tableName);
        assertEquals(table.getType(), AttributeOwnerType.TABLE.getValue());
        assertNotNull(table.getLastModifiedKey());
        assertNotNull(table.getPrimaryKey());
    }

    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] {
                { CUSTOMERSPACE1, TABLE1 },
                { CUSTOMERSPACE2, TABLE2 },
        };
    }
}
