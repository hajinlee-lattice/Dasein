package com.latticeengines.dataplatform.exposed.domain;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.util.JsonHelper;

public class ThrottleConfigurationUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        
        String configString = config.toString();
        ThrottleConfiguration deserializedConfig = (ThrottleConfiguration) JsonHelper.deserialize(configString, ThrottleConfiguration.class);
        assertTrue(deserializedConfig.isImmediate());
        assertEquals(5, deserializedConfig.getJobRankCutoff());
    }
}
