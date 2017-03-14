package com.latticeengines.pls.controller;

import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class AccountResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private MetadataProxy metadataProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        Table table = createTestTable();
        metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
        DataCollection collection = new DataCollection();
        collection.setType(DataCollectionType.Segmentation);
        collection.setTables(Collections.singletonList(table));
        metadataProxy.createDataCollection(mainTestTenant.getId(), collection);
    }

    @Test(groups = "deployment", enabled = false)
    public void testGetCount() {
        FrontEndQuery query = new FrontEndQuery();
        try (PerformanceTimer timer = new PerformanceTimer("testGetCount")) {
            long count = restTemplate.postForObject(getRestAPIHostPort() + "/pls/accounts/count", query, Long.class);
            assertTrue(count > 0);
        }
    }

    private Table createTestTable() {
        Table table = new Table();
        table.setInterpretation(SchemaInterpretation.Account.toString());
        table.setName("querytest_table");
        table.setDisplayName("querytest_table");
        Attribute companyName = createAttribute("companyname");
        table.addAttribute(companyName);
        Attribute city = createAttribute("city");
        table.addAttribute(city);
        Attribute state = createAttribute("state");
        table.addAttribute(state);
        Attribute lastName = createAttribute("lastname");
        table.addAttribute(lastName);
        Attribute familyMembers = createAttribute("number_of_family_members");
        table.addAttribute(familyMembers);
        JdbcStorage storage = new JdbcStorage();
        storage.setTableNameInStorage("querytest_table");
        storage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        table.setStorageMechanism(storage);
        return table;
    }

    private Attribute createAttribute(String name) {
        Attribute attribute = new Attribute();
        attribute.setName(name);
        attribute.setDisplayName(name);
        attribute.setPhysicalDataType("String");
        return attribute;
    }
}
