package com.latticeengines.common.exposed.util;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import reactor.core.publisher.Flux;

public class PartitionUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testPartitionBySize() {
        List<Integer> collection = Flux.range(0, 20).collectList().block();
        List<List<Integer>> partitions = PartitionUtils.partitionBySize(collection, 3);
        Assert.assertEquals(partitions.size(), 7);
        partitions.forEach(partition -> Assert.assertTrue(partition.size() <= 3));
    }

}
