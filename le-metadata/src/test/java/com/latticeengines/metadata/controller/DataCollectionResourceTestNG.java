package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.impl.RegisterAccountMasterMetadataTableTestNG;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

public class DataCollectionResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final DataCollection DATA_COLLECTION = new DataCollection();
    private static final String COLLECTION_NAME = "ApiTestCollection";
    private static final Table TABLE_1 = new Table();

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private RegisterAccountMasterMetadataTableTestNG registerAccountMasterMetadataTableTestNG;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
        registerAccountMasterMetadataTableTestNG.registerMetadataTable();
        DATA_COLLECTION.setName(COLLECTION_NAME);
    }

    @Test(groups = "functional")
    public void createDataCollection_assertCreated() {
        DATA_COLLECTION.setType(DataCollectionType.Segmentation);
        DATA_COLLECTION.setTables(Collections.singletonList(TABLE_1));

        System.out.println("Data collection is: " + JsonUtils.serialize(DATA_COLLECTION));
        dataCollectionProxy.createOrUpdateDataCollection(customerSpace1, DATA_COLLECTION);

        DataCollection retrieved = dataCollectionProxy.getDataCollection(customerSpace1, DATA_COLLECTION.getName());
        assertNotNull(retrieved);
//        assertEquals(retrieved.getTables().size(), 2);
//        assertNotNull(retrieved.getTable(SchemaInterpretation.AccountMaster));
        assertEquals(retrieved.getType(), DataCollectionType.Segmentation);
    }

    @Test(groups = "functional", dependsOnMethods = "createDataCollection_assertCreated", enabled = false)
    public void createStatistics() {
//        Statistics statistics = new Statistics();
//        StatisticsContainer container = new StatisticsContainer();
//        container.setStatistics(statistics);
//        dataCollectionProxy.upsertMainStats(customerSpace1, DATA_COLLECTION.getName(), container, true);
//        DataCollection retrieved = dataCollectionProxy.getDataCollection(customerSpace1, DATA_COLLECTION.getName());
//        assertNotNull(retrieved.getStatisticsContainer());
    }
}
