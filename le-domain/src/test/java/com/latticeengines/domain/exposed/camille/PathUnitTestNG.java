package com.latticeengines.domain.exposed.camille;

import java.util.ArrayList;

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

    @Test(groups = "unit")
    public void testAppend() {
        Path p = new Path("/foo/bar/baz");
        p = p.append("goo");
        Assert.assertEquals(p.toString(), "/foo/bar/baz/goo");
    }

    @Test(groups = "unit")
    public void testAppendPath() {
        Path p = new Path("/foo/bar/baz");
        p = p.append(new Path("/goo"));
        Assert.assertEquals(p.toString(), "/foo/bar/baz/goo");
    }

    @Test(groups = "unit")
    public void testPrefixPath() {
        Path p = new Path("/foo/bar/baz");
        p = p.prefix(new Path("/goo"));
        Assert.assertEquals(p.toString(), "/goo/foo/bar/baz");
    }

    @Test(groups = "unit")
    public void testEquals() {
        Path p1 = new Path("/foo/bar/baz");
        Path p2 = new Path("/foo/bar/baz");
        Assert.assertEquals(p1, p2);
    }

    @Test(groups = "unit")
    public void testNonequalPathsDifferentLengths() {
        Path p1 = new Path("/foo/bar/baz");
        Path p2 = new Path("/foo/bar/baz/d");
        Assert.assertNotEquals(p1, p2);
    }

    @Test(groups = "unit")
    public void testNonequalPathsSameLengths() {
        Path p1 = new Path("/foo/bar/baz");
        Path p2 = new Path("/foo/bar/bar");
        Assert.assertNotEquals(p1, p2);
    }

    @Test(groups = "unit")
    public void testPathNotEqualsNull() {
        Path p = new Path("/foo/bar/baz");
        Assert.assertNotEquals(p, null);
    }

    @Test(groups = "unit")
    public void testDefaultSpaceFile() {
        @SuppressWarnings("unused")
        Path p = new Path("foo", ".default-space");
    }

    @Test(groups = "unit")
    public void testLocalPath() {
        Path path = new Path("/a/b/c/d");
        Path local = path.local(new Path("/a/b"));
        Assert.assertEquals(local.toString(), "/c/d");
    }

    @Test(groups = "unit")
    public void testStartsWith() {
        Path path = new Path("/a/b/c/d");
        Assert.assertTrue(path.startsWith(new Path("/a/b")));
        Assert.assertFalse(path.startsWith(new Path("/a/b/c/d/e")));
        Assert.assertFalse(path.startsWith(new Path("/foo/bar")));
    }

    @Test(groups = "unit")
    public void testRootPath() {
        Path path = new Path("/");
        Assert.assertEquals(path.toString(), "/");
        path = new Path(new ArrayList<String>());
        Assert.assertEquals(path.toString(), "/");
        Assert.assertTrue(new Path("/a/b/c").startsWith(new Path("/")));
        Assert.assertEquals(new Path("/a/b/c").local(new Path("/")).toString(), "/a/b/c");
        Assert.assertEquals(new Path("/").getSuffix(), "");
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void testParentForRootPathIsInvalid() {
        Path p = new Path("/");
        p.parent();
    }
}
