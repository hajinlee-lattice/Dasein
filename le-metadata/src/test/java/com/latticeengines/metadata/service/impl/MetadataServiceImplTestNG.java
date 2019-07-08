package com.latticeengines.metadata.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Direction;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;

public class MetadataServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MetadataServiceImplTestNG.class);

    @Autowired
    private MetadataService mdService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", dataProvider = "tableProvider")
    public void getTable(String customerSpace, String tableName) {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, true);
        assertNotNull(table);
        assertEquals(table.getName(), tableName);
        assertNotNull(table.getLastModifiedKey());
        assertNotNull(table.getPrimaryKey());
        assertNotNull(table.getAttributes());
        log.info("Attribute Count for Table: {} - {}", tableName, table.getAttributes().size());
        assertTrue(table.getAttributes().size() > 0);
        assertEquals(table.getStorageMechanism().getName(), "HDFS");
        assertTrue(table.getAttributes().size() == table.getColumnMetadata().size());

        // Get without attributes
        table = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, false);
        assertNotNull(table);
        assertNotNull(table.getAttributes());
        assertTrue(table.getAttributes().size() == 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "getTable" })
    public void getTableAttributes() {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1, true);
        assertNotNull(table.getAttributes());

        Pageable pageReq = PageRequest.of(0, 50000);
        List<Attribute> colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1,
                pageReq);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), table.getAttributes().size());

        // With Pageable as Null
        colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1, null);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), table.getAttributes().size());

        pageReq = PageRequest.of(0, 10);
        colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1, pageReq);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), 10);

        pageReq = PageRequest.of(0, 10, Direction.ASC, "displayName");
        colMetaList = mdService.getTableAttributes(CustomerSpace.parse(customerSpace1), TABLE1, pageReq);
        assertNotNull(colMetaList);
        assertEquals(colMetaList.size(), 10);
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

    @Test(groups = "functional", dependsOnMethods = { "addStorageMechanism" })
    public void cloneTable() throws IOException {
        Table cloned = mdService.cloneTable(CustomerSpace.parse(customerSpace1), TABLE1, false);
        assertNotNull(cloned);
        List<Extract> extracts = cloned.getExtracts();
        Assert.assertNotNull(extracts);
        Assert.assertEquals(extracts.size(), 1);
        Extract extract = extracts.get(0);
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, extract.getPath());
        Assert.assertNotNull(files);
        Assert.assertEquals(files.size(), 2);
        String[] expected = { "Extract1_0.avro", "Extract2_1.avro" };
        Set<String> expectedSet = new HashSet<>(Arrays.asList(expected));
        files.forEach(file -> {
            String fileName = new org.apache.hadoop.fs.Path(file).getName();
            Assert.assertTrue(expectedSet.contains(fileName));
        });
    }

    @Test(groups = "functional", dependsOnMethods = { "cloneTable" })
    public void renameTable() {
        Table table = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1, true);
        assertNotNull(table);
        String newName = table.getName() + "-rename1";
        mdService.renameTable(CustomerSpace.parse(customerSpace1), table.getName(), newName);
        Table newTable = mdService.getTable(CustomerSpace.parse(customerSpace1), newName, true);
        assertEquals(newTable.getPid(), table.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "renameTable" })
    public void fixTableAttribute() {
        Table newTable = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1 + "-rename1", true);
        Long tablePid = newTable.getPid();
        Assert.assertNotNull(newTable);
        Attribute attr1 = newTable.getAttribute("OwnerId");
        Attribute attr2 = newTable.getAttribute("CreatedDate");

        Assert.assertNotNull(attr1);
        Assert.assertNotNull(attr2);
        Assert.assertEquals(attr1.getPhysicalDataType(), "string");
        Assert.assertTrue(StringUtils.isEmpty(attr1.getFundamentalType()));
        Assert.assertNull(attr2.getDateFormatString());
        AttributeFixer attributeFixer1 = new AttributeFixer();
        attributeFixer1.setName("OwnerId");
        attributeFixer1.setPhysicalDataType("Int");
        attributeFixer1.setFundamentalType(FundamentalType.NUMERIC);

        AttributeFixer attributeFixer2 = new AttributeFixer();
        attributeFixer2.setName("CreatedDate");
        attributeFixer2.setDateFormat("MM/DD/YYYY");

        mdService.fixAttributes(CustomerSpace.parse(customerSpace1), newTable.getName(),
                Arrays.asList(attributeFixer1, attributeFixer2));
        newTable = mdService.getTable(CustomerSpace.parse(customerSpace1), TABLE1 + "-rename1", true);
        Assert.assertEquals(newTable.getPid(), tablePid);

        attr1 = newTable.getAttribute("OwnerId");
        attr2 = newTable.getAttribute("CreatedDate");

        Assert.assertNotNull(attr1);
        Assert.assertNotNull(attr2);
        Assert.assertEquals(attr1.getPhysicalDataType(), "Int");
        Assert.assertEquals(attr1.getFundamentalType(), FundamentalType.NUMERIC.name());
        Assert.assertEquals(attr2.getDateFormatString(), "MM/DD/YYYY");

    }

    @DataProvider(name = "tableProvider")
    public Object[][] tableProvider() {
        return new Object[][] { { customerSpace1, TABLE1 }, { customerSpace2, TABLE1 }, };
    }
}
