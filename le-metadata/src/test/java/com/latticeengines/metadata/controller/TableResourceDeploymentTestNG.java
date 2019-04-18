package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

public class TableResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TableResourceDeploymentTestNG.class);

    private static final String TABLE_NAME = "TestAccountTable";
    private static final String S3_DIR = "le-dev/LocalTest";
    private static final String S3_VERSION = "4";
    private static String renamedTableName = TABLE_NAME;

    @Inject
    private TestArtifactService testArtifactService;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();
        log.info("Verifying Table CRUD ops with Tenant: " + customerSpace1);
        log.info("------------------------------------------------");
    }

    @Test(groups = "deployment")
    public void testCheckTable() {
        Table testTable = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNull(testTable);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCheckTable")
    public void testCreateTable() {
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, BusinessEntity.Account.getServingStore().name() + ".json");
        if (is == null) {
            throw new RuntimeException("Could not download Account json from S3");
        }

        Table table;
        try {
            ObjectMapper om = new ObjectMapper();
            List<?> list = om.readValue(is, List.class);
            table = JsonUtils.convertList(list, Table.class).get(0);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse table from " + BusinessEntity.Account.getServingStore().name() + ".json.", e);
        }
        assertNotNull(table, "Table Object is empty");
        assertNotNull(table.getAttributes(), "Table Attributes are empty");
        logTableSummary("Table skeleton from S3", table);

        assertTrue(table.getAttributes().size() >= 10100, "Doesn't have minimum attributes 10100 for this test");
        List<Attribute> minAttributes = table.getAttributes().subList(0,  10100);

        table.setAttributes(minAttributes);
        table.setName(TABLE_NAME);
        table.setDisplayName(customerSpace1 + TABLE_NAME);
        table.setTenant(tenant1);
        logTableSummary("Creating Table", table);

        metadataProxy.createTable(customerSpace1, TABLE_NAME, table);
        Table tableFromDB = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNotNull(tableFromDB, "Retrieved Table from DB is empty");
        logTableSummary("Account Table Summary from DB", tableFromDB);
        assertEquals(minAttributes.size(), tableFromDB.getAttributes().size());
    }

    private void logTableSummary(String context, Table table) {
        log.info("{}. Name: {}, Attribute Count: {} ", context, table.getName(), table.getAttributes().size());
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreateTable")
    public void testGetTables() {
        List<String> tables = metadataProxy.getTableNames(customerSpace1);

        assertNotNull(tables);
        log.info("Available Table Names : {} for tenant: {}", tables, customerSpace1);
        assertTrue(tables.contains(TABLE_NAME));
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetTables")
    public void testUpdateTable() {
        Table srcTable = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNotNull(srcTable);

        // Update the table by adding 1 attribute
        log.info("Updating {} for {}", TABLE_NAME,  customerSpace1);
        Attribute attribute = new Attribute();
        attribute.setName("Name1");
        attribute.setDisplayName("DisplayName1");
        attribute.setPhysicalDataType("PhysicalDataType1");
        srcTable.addAttribute(attribute);

        String tableDisplayName = customerSpace1 + TABLE_NAME + "-update1";
        srcTable.setDisplayName(tableDisplayName);
        metadataProxy.updateTable(customerSpace1, TABLE_NAME, srcTable);

        final Table updatedTable = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNotNull(updatedTable);
        assertEquals(updatedTable.getDisplayName(), tableDisplayName);
        logTableSummary("Updated Table from DB", updatedTable);
        assertEquals(updatedTable.getAttributes().size(), srcTable.getAttributes().size(), "Updated table attribute count doesn't match for add usecase");

        attribute = updatedTable.getAttribute("Name1");
        assertNotNull(attribute);
        assertEquals(attribute.getDisplayName(), "DisplayName1");
        assertEquals(attribute.getPhysicalDataType(), "PhysicalDataType1");

        // Update the table by deleting 5 attributes
        List<String> deletedAttributes = new ArrayList<>();
        IntStream.range(0 , 5).forEach(ctr -> {
            Attribute delAttr = updatedTable.getAttributes().get(new Random().nextInt(updatedTable.getAttributes().size()));
            updatedTable.removeAttribute(delAttr.getName());
            deletedAttributes.add(delAttr.getName());
        });
        log.info("Deleted Attributes from Source Table: {}", deletedAttributes);

        tableDisplayName = customerSpace1 + TABLE_NAME + "-update2";
        updatedTable.setDisplayName(tableDisplayName);
        metadataProxy.updateTable(customerSpace1, TABLE_NAME, updatedTable);

        srcTable = updatedTable;
        Table updatedTable2 = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNotNull(updatedTable2);
        assertEquals(updatedTable2.getDisplayName(), tableDisplayName);
        logTableSummary("Updated Table after removing 5 attrinutes", updatedTable2);
        assertEquals(updatedTable2.getAttributes().size(), srcTable.getAttributes().size(), "Updated table attribute count doesn't match for delete usecase");
        // Verify deleted attributes
        for (String delAttrName : deletedAttributes) {
            Attribute attr = updatedTable2.getAttribute(delAttrName);
            assertNull(attr, "Attribute should not exist after update");
        }

        // Update all Attributes
        String updateSuffix = "-Updated";
        updatedTable2.getAttributes().forEach(attr -> {
            attr.setDisplayName(attr.getDisplayName() + updateSuffix);
        });
        metadataProxy.updateTable(customerSpace1, TABLE_NAME, updatedTable2);

        srcTable = updatedTable2;
        updatedTable2 = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNotNull(updatedTable2);
        logTableSummary("Updated Table after Attribute updates", updatedTable2);
        assertEquals(updatedTable2.getAttributes().size(), srcTable.getAttributes().size(), "Updated table attribute count doesn't match for attribute update usecase");
        updatedTable2.getAttributes().forEach(attr -> {
            assertTrue(attr.getDisplayName().endsWith(updateSuffix));
        });
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateTable")
    public void testRenameTable() {
        Table srcTable = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNotNull(srcTable);
        String newTableName = TABLE_NAME + "-rename1";
        metadataProxy.updateTable(customerSpace1, newTableName, srcTable);
        Table updatedTable = metadataProxy.getTable(customerSpace1, TABLE_NAME);
        assertNull(updatedTable, "Even after rename table, old table exists");
        updatedTable = metadataProxy.getTable(customerSpace1, newTableName);
        assertNotNull(updatedTable, "Couldn't find the renamed table");
        assertEquals(updatedTable.getAttributes().size(), srcTable.getAttributes().size(), "Renamed table attribute count doesn't match after Table Rename");

        String newTableName2 = TABLE_NAME + "-rename2";
        metadataProxy.renameTable(customerSpace1, newTableName, newTableName2);
        Table updatedTable2 = metadataProxy.getTable(customerSpace1, newTableName);
        assertNull(updatedTable2, "Even after rename table, old table exists");
        updatedTable2 = metadataProxy.getTable(customerSpace1, newTableName2);
        assertNotNull(updatedTable2, "Couldn't find the renamed table");
        assertEquals(updatedTable2.getAttributes().size(), updatedTable.getAttributes().size(), "Renamed table attribute count doesn't match after Table Rename");
        assertEquals(updatedTable2.getPid(), updatedTable.getPid(), "Rename should not change internal IDs");
        // Will be used in deleteTable method
        renamedTableName = newTableName2;
    }

    @Test(groups = "deployment", dependsOnMethods = "testRenameTable")
    public void testDeleteTable() {
        log.info("Deleting {} for {} ", renamedTableName, customerSpace1);
        metadataProxy.deleteTable(customerSpace1, renamedTableName);
        Table table = metadataProxy.getTable(customerSpace1, renamedTableName);

        assertNull(table);
    }
}
