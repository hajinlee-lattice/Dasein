package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.common.exposed.util.JsonUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class DataCollectionResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final String BASE_URL_DATA_COLLECTION = "%s/metadata/customerspaces/%s/datacollections/";
    private static final DataCollection DATA_COLLECTION = new DataCollection();
    private static final DataCollection DATA_COLLECTION_1 = new DataCollection();
    private static final Table TABLE_1 = new Table();

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        TABLE_1.setName(TABLE1);
    }

    @Test(groups = "functional")
    public void createDataCollection_assertCreated() {
        DATA_COLLECTION.setType(DataCollectionType.Segmentation);
        DATA_COLLECTION.setTables(Collections.singletonList(TABLE_1));

        System.out.println("Data collection is: " + JsonUtils.serialize(DATA_COLLECTION));
        restTemplate.postForObject(
                String.format(BASE_URL_DATA_COLLECTION, getRestAPIHostPort(), CUSTOMERSPACE1),
                DATA_COLLECTION, DataCollection.class);

        DataCollection retrieved = restTemplate.getForObject(
                String.format(BASE_URL_DATA_COLLECTION + "types/%s", getRestAPIHostPort(),
                        CUSTOMERSPACE1, DataCollectionType.Segmentation.toString()),
                DataCollection.class);
        assertNotNull(retrieved);
        assertEquals(retrieved.getTables().size(), 1);
        assertEquals(retrieved.getType(), DataCollectionType.Segmentation);
    }

}
