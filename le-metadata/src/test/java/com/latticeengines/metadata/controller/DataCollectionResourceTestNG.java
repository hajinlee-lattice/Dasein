package com.latticeengines.metadata.controller;

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
    public void getDefaultDataCollection() {
        DataCollection collection = dataCollectionProxy.getDefaultDataCollection(customerSpace1);
        DataCollection.Version version = dataCollectionEntityMgr.getActiveVersion();
        Assert.assertEquals(collection.getName(), dataCollection.getName());
        dataCollectionProxy.upsertTable(customerSpace1, TABLE1, TableRoleInCollection.ConsolidatedAccount, version);
        Table table = dataCollectionProxy.getTable(customerSpace1, TableRoleInCollection.ConsolidatedAccount);
        Assert.assertNotNull(table);
    }
}
