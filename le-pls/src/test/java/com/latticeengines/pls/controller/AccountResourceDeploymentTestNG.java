package com.latticeengines.pls.controller;

import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.frontend.FlattenedRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class AccountResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private MetadataProxy metadataProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        Table table = new Table();
        table.setInterpretation(SchemaInterpretation.Account.toString());
        table.setName("querytest_table");
        table.setDisplayName("Foo");
        JdbcStorage storage = new JdbcStorage();
        storage.setTableNameInStorage("querytest_table");
        storage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        table.setStorageMechanism(storage);
        metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
        metadataProxy.createDefaultDataCollection(mainTestTenant.getId(), "",
                Collections.singletonList("querytest_table"));
    }

    @Test(groups = "deployment", enabled = false)
    public void testGetCount() {
        FrontEndQuery query = new FrontEndQuery();
        query.setRestriction(new FlattenedRestriction());
        long count = restTemplate.postForObject(getRestAPIHostPort() + "/pls/accounts/count", query, Long.class);
        assertTrue(count > 0);
    }
}
