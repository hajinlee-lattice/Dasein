package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;

public class ThrottleConfigurationUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        
        String configString = config.toString();
        ThrottleConfiguration deserializedConfig = (ThrottleConfiguration) JsonUtils.deserialize(configString, ThrottleConfiguration.class);
        assertTrue(deserializedConfig.isImmediate());
        assertEquals(Integer.valueOf(5), deserializedConfig.getJobRankCutoff());
    }
}
