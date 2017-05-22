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
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.impl.RegisterAccountMasterMetadataTableTestNG;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.StatisticsContainerProxy;

public class DataCollectionResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final String BASE_URL_DATA_COLLECTION = "%s/metadata/customerspaces/%s/datacollections/";
    private static final DataCollection DATA_COLLECTION = new DataCollection();
    private static final DataCollection DATA_COLLECTION_1 = new DataCollection();
    private static final Table TABLE_1 = new Table();

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private RegisterAccountMasterMetadataTableTestNG registerAccountMasterMetadataTableTestNG;
    @Autowired
    private StatisticsContainerProxy statisticsContaineProxy;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
        registerAccountMasterMetadataTableTestNG.registerMetadataTable();
    }

    @Test(groups = "functional")
    public void createDataCollection_assertCreated() {
        DATA_COLLECTION.setType(DataCollectionType.Segmentation);
        DATA_COLLECTION.setTables(Collections.singletonList(TABLE_1));

        System.out.println("Data collection is: " + JsonUtils.serialize(DATA_COLLECTION));
        dataCollectionProxy.createOrUpdateDataCollection(customerSpace1, DATA_COLLECTION);

        DataCollection retrieved = dataCollectionProxy.getDataCollectionByType(customerSpace1,
                DATA_COLLECTION.getType());
        assertNotNull(retrieved);
        assertEquals(retrieved.getTables().size(), 2);
        assertNotNull(retrieved.getTable(SchemaInterpretation.BucketedAccountMaster));
        assertEquals(retrieved.getType(), DataCollectionType.Segmentation);

        retrieved = dataCollectionProxy.getDataCollectionByType(customerSpace1, DATA_COLLECTION.getType());
        assertNotNull(retrieved);
        assertEquals(retrieved.getTables().size(), 2);
        assertNotNull(retrieved.getTable(SchemaInterpretation.BucketedAccountMaster));
        assertEquals(retrieved.getType(), DataCollectionType.Segmentation);
    }

    @Test(groups = "functional", dependsOnMethods = "createDataCollection_assertCreated")
    public void createStatistics() {
        DataCollection retrieved = dataCollectionProxy.getDataCollectionByType(customerSpace1,
                DATA_COLLECTION.getType());
        Statistics statistics = new Statistics();
        StatisticsContainer container = new StatisticsContainer();
        container.setStatistics(statistics);
        container = statisticsContaineProxy.createOrUpdateStatistics(customerSpace1, container);

        retrieved.setStatisticsContainer(container);
        dataCollectionProxy.createOrUpdateDataCollection(customerSpace1, retrieved);

        retrieved = dataCollectionProxy.getDataCollectionByType(customerSpace1, DATA_COLLECTION.getType());
        assertNotNull(retrieved.getStatisticsContainer());
    }
}
