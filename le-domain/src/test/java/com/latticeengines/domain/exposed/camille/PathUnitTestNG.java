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
    
    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidParts1() {
        @SuppressWarnings("unused")
        Path p = new Path("/foo", "/baz");
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidParts2() {
        @SuppressWarnings("unused")
        Path p = new Path("/foo/", "/baz/");
    }
    
    @Test(groups = "unit")
    public void testValidPath() {
        @SuppressWarnings("unused")
        Path p = new Path("/foo/bar");
    }
    
    @Test(groups = "unit")
    public void testSuffix() {
        Path p = new Path("/foo/bar/baz");
        Assert.assertEquals(p.getSuffix(), "baz");
    }
    
    @Test(groups = "unit")
    public void testToString() {
        Path p = new Path("/foo/bar/baz");
        Assert.assertEquals(p.toString(), "/foo/bar/baz");
    }
}
