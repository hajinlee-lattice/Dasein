package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DimensionsUnitTestNG {

    @Test(groups = "unit")
    public void testEquals() {
        List<Long> dimensions1 = new ArrayList<>();
        dimensions1.add(5L);
        dimensions1.add(100L);
        List<Long> dimensions2 = new ArrayList<>();
        dimensions2.add(5L);
        dimensions2.add(100L);
        List<Long> dimensions3 = new ArrayList<>();
        dimensions3.add(6L);
        dimensions3.add(100L);
        List<Long> dimensions4 = new ArrayList<>();
        dimensions4.add(5L);
        dimensions4.add(101L);

        Dimensions dim1 = new Dimensions(dimensions1);
        Dimensions dim2 = new Dimensions(dimensions2);
        Dimensions dim3 = new Dimensions(dimensions3);
        Dimensions dim4 = new Dimensions(dimensions4);

        Assert.assertEquals(dim1, dim2);
        Assert.assertNotEquals(dim1, dim3);
        Assert.assertNotEquals(dim1, dim4);
        Assert.assertNotEquals(dim3, dim4);
    }
}
