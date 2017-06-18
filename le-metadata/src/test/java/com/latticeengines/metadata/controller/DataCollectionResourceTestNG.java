package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.metadata.functionalframework.DataCollectionFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

public class DataCollectionResourceTestNG extends DataCollectionFunctionalTestNGBase {

    private static final DataCollection DATA_COLLECTION = new DataCollection();
    private static final String COLLECTION_NAME = "ApiTestCollection";
    private static final Table TABLE_1 = new Table();

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
        DATA_COLLECTION.setName(COLLECTION_NAME);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        super.cleanup();
    }

    @Test(groups = "functional")
    public void createDataCollection_assertCreated() {
        dataCollectionProxy.createOrUpdateDataCollection(customerSpace1, DATA_COLLECTION);
        dataCollectionProxy.upsertTable(customerSpace1, DATA_COLLECTION.getName(), TABLE1, TableRoleInCollection.ConsolidatedAccount);

        DataCollection retrieved = dataCollectionProxy.getDataCollection(customerSpace1, DATA_COLLECTION.getName());
        assertNotNull(retrieved);
        assertEquals(retrieved.getName(), COLLECTION_NAME);

        List<Table> tables = dataCollectionProxy.getAllTables(customerSpace1, DATA_COLLECTION.getName());
        Assert.assertEquals(tables.size(), 1);
    }
}
