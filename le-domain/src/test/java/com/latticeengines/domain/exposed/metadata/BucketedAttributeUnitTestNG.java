package com.latticeengines.domain.exposed.metadata;

import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class BucketedAttributeUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() {
        BucketedAttribute attr = new BucketedAttribute("nominalAttr", Collections.emptyList(), //
                0, 3);
        String serialized = JsonUtils.serialize(attr);
        Assert.assertTrue(serialized.contains("\"num_bits\":3"));
        BucketedAttribute deserialized = JsonUtils.deserialize(serialized, BucketedAttribute.class);
        Assert.assertEquals(deserialized.getNumBits(), 3);
        System.out.println(serialized);
    }
}
