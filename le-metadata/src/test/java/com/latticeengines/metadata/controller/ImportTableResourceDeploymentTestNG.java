package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;

public class ImportTableResourceDeploymentTestNG extends MetadataDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ImportTableResourceDeploymentTestNG.class);

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();
    }

    @Test(groups = "deployment")
    public void testGetTable() throws IOException {
        Table table = new Table();
        table.setName(TABLE2);
        table.setDisplayName(TABLE2);
        table.setTenant(tenant1);

        log.info("Creating TABLE2 for " + customerSpace1);
        metadataProxy.createImportTable(customerSpace1, TABLE2, table);
        Table testTable = metadataProxy.getImportTable(customerSpace1, TABLE2);

        assertNotNull(testTable);
        assertEquals(testTable.getName(), TABLE2);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetTable")
    public void testGetTables() throws IOException {
        List<String> tables = metadataProxy.getImportTableNames(customerSpace1);

        assertNotNull(tables);
        assertTrue(tables.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetTables")
    public void testUpdateTable() throws IOException {
        Table table = metadataProxy.getImportTable(customerSpace1, TABLE2);
        assertNotNull(table);

        log.info("Updating TABLE2 for " + customerSpace1);
        Attribute attribute = new Attribute();
        attribute.setName("Name1");
        attribute.setDisplayName("DisplayName1");
        attribute.setPhysicalDataType("PhysicalDataType1");
        table.addAttribute(attribute);
        table.setDisplayName(TABLE2 + "test");
        metadataProxy.updateImportTable(customerSpace1, TABLE2, table);

        table = metadataProxy.getImportTable(customerSpace1, TABLE2);
        assertNotNull(table);
        assertEquals(table.getDisplayName(),TABLE2 + "test");

        attribute = table.getAttribute("Name1");
        assertNotNull(attribute);
        assertEquals(attribute.getDisplayName(),"DisplayName1");
        assertEquals(attribute.getPhysicalDataType(),"PhysicalDataType1");
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdateTable")
    public void testGetTableMetadata() throws IOException {
        ModelingMetadata modelingMetadata = metadataProxy.getTableMetadata(customerSpace1, TABLE2);
        assertNotNull(modelingMetadata);

        List<ModelingMetadata.AttributeMetadata> attributes = modelingMetadata.getAttributeMetadata();
        assertNotNull(attributes);
        assertTrue(attributes.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetTableMetadata")
    public void testDeleteTable() throws IOException {
        log.info("Deleting TABLE2 for " + customerSpace1);
        metadataProxy.deleteImportTable(customerSpace1, TABLE2);
        Table table = metadataProxy.getImportTable(customerSpace1, TABLE2);

        assertNull(table);
    }
}
