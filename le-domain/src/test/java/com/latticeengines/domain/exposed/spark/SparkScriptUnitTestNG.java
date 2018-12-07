package com.latticeengines.domain.exposed.spark;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class SparkScriptUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        InputStreamSparkScript script = new InputStreamSparkScript();
        String serialized = JsonUtils.serialize(script);
        SparkScript deserialized = JsonUtils.deserialize(serialized, SparkScript.class);
        Assert.assertTrue(deserialized instanceof InputStreamSparkScript);
    }

}
