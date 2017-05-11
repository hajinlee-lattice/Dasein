package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;

public class MetadataServiceImplTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private MetadataService mdService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void getTable(String customerSpace, String tableName) {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace), tableName);
        assertNotNull(table);
        assertEquals(table.getName(), tableName);
        assertNotNull(table.getLastModifiedKey());
        assertNotNull(table.getPrimaryKey());
        assertEquals(table.getStorageMechanism().getName(), "HDFS");
    }

    @Test(groups = "functional")
    public void getTables() {
        List<Table> tables = mdService.getTables(CustomerSpace.parse(customerSpace1));
        assertEquals(tables.size(), 1);
    }
    
    @Test(groups = "functional", dependsOnMethods = { "getTables" })
    public void addStorageMechanism() {
        Table table = mdService.getTables(CustomerSpace.parse(customerSpace1)).get(0);
        JdbcStorage jdbcStorage = new JdbcStorage();
        jdbcStorage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        jdbcStorage.setTableNameInStorage("TABLE1_IN_REDSHIFT");
        mdService.setStorageMechanism(CustomerSpace.parse(customerSpace1), table.getName(), jdbcStorage);
        
        Table retrievedTable = mdService.getTables(CustomerSpace.parse(customerSpace1)).get(0);
        JdbcStorage storageMechanism = (JdbcStorage) retrievedTable.getStorageMechanism();
        assertEquals(storageMechanism.getDatabaseName(), JdbcStorage.DatabaseName.REDSHIFT);
    }

    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] { {customerSpace1, TABLE1 }, {customerSpace2, TABLE1 }, };
    }
}
