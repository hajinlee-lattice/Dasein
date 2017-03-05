package com.latticeengines.domain.exposed.datacloud.dataflow;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class BucketAlgorithmUnitTestNG {

    @Test(groups = "unit")
    public void test() {
        CategoricalBucket cBucket = new CategoricalBucket();
        IntervalBucket iBucket = new IntervalBucket();

        String cBktSer = JsonUtils.serialize(cBucket);
        String iBktSer = JsonUtils.serialize(iBucket);

        BucketAlgorithm algo = JsonUtils.deserialize(cBktSer, BucketAlgorithm.class);
        Assert.assertTrue(algo instanceof CategoricalBucket);
        algo = JsonUtils.deserialize(iBktSer, BucketAlgorithm.class);
        Assert.assertTrue(algo instanceof IntervalBucket);
    }

}
