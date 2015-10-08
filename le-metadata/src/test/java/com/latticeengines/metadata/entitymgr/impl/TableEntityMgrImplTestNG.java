package com.latticeengines.metadata.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
        assertEquals(tables.get(0).getName(), tableName);
    }
    
    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] {
                { CUSTOMERSPACE1, TABLE1 },
                { CUSTOMERSPACE2, TABLE2 },
        };
    }

}
