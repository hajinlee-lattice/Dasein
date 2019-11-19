package com.latticeengines.domain.exposed.modelreview;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DataRuleUnitTestNG {

    @Test(groups = "unit")
    private void testSerDe() {
        DataRule rule = new DataRule();
        rule.setDisplayName("test");

        String ruleStr = JsonUtils.serialize(rule);
        Assert.assertNotNull(ruleStr);
        Assert.assertTrue(ruleStr.contains("id"), "should contain id field from graph node in serialized str");

        DataRule deserializedRule = JsonUtils.deserialize(ruleStr, DataRule.class);
        Assert.assertNotNull(deserializedRule);
        Assert.assertNotEquals(deserializedRule.getId(), rule.getId());
    }
}
