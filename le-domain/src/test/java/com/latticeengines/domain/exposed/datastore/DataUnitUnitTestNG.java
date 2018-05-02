package com.latticeengines.domain.exposed.datastore;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;

public class DataUnitUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        DynamoDataUnit dynamoDataUnit = new DynamoDataUnit();
        String serialized = JsonUtils.serialize(dynamoDataUnit);
        System.out.println(serialized);
        DataUnit deserialized = JsonUtils.deserialize(serialized, DataUnit.class);
        Assert.assertTrue(deserialized instanceof DynamoDataUnit);
    }

}
