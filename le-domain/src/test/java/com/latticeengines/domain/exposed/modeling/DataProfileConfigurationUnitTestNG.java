package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DataProfileConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer("INTERNAL");
        config.setTable("iris");
        config.setMetadataTable("iris_metadata");
        config.setSamplePrefix("all");
        
        String jsonString = config.toString();
        System.out.println(jsonString);
        DataProfileConfiguration deserializedConfig = JsonUtils.deserialize(jsonString, DataProfileConfiguration.class);
        assertEquals(deserializedConfig.toString(), jsonString);

    }
}
