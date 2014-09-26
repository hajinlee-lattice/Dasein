package com.latticeengines.domain.exposed.camille;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.algorithm.DecisionTreeAlgorithm;


public class PathUnitTestNG {

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath1() {
        Path p = new Path("badpath");
    }
    
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath2() {
        Path p = new Path("//badpath");
    }
    
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath3() {
        Path p = new Path("/badpath*");
    }
    
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath4() {
        Path p = new Path("C:\\Windows\\badpath");
    }
    
    @Test(groups = "unit")
    public void testValidPath() {
        Path p = new Path("/foo/bar");
    }
    
    @Test(groups = "unit")
    public void testSuffix() {
        Path p = new Path("/foo/bar/baz");
        Assert.assertEquals("baz", p.getSuffix());
    }
    
    @Test(groups = "unit")
    public void testToString() {
        Path p = new Path("/foo/bar/baz");
        Assert.assertEquals("/foo/bar/baz", p.toString());
    }
}
