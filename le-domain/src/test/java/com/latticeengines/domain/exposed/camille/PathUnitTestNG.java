package com.latticeengines.domain.exposed.camille;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PathUnitTestNG {

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath1() {
        @SuppressWarnings("unused")
        Path p = new Path("badpath");
    }
    
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath2() {
        @SuppressWarnings("unused")
        Path p = new Path("//badpath");
    }
    
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath3() {
        @SuppressWarnings("unused")
        Path p = new Path("/badpath*");
    }
    
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPath4() {
        @SuppressWarnings("unused")
        Path p = new Path("C:\\Windows\\badpath");
    }
    
    @Test(groups = "unit")
    public void testValidPath() {
        @SuppressWarnings("unused")
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
