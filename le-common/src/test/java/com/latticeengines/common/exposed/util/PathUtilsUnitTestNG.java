package com.latticeengines.common.exposed.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PathUtilsUnitTestNG {

    @Test(groups = { "unit", "functional" })
    public void testStripoutProtocol() throws Exception {
        String path = "hdfs://localhost:9000/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/Lead/Extracts/2015-10-21-22-40-34/Lead-2015-10-21.avro";
        Assert.assertEquals(
                PathUtils.stripoutProtocol(path),
                "/Pods/Default/Contracts/DemoContract/Tenants/DemoTenant/Spaces/Production/Data/Tables/Lead/Extracts/2015-10-21-22-40-34/Lead-2015-10-21.avro");
    }

    @Test(groups = "unit", dataProvider = "toNestedDirGlob")
    private void testToNestedDirGlob(String path, int nNestedDirs, String expectedGlob) {
        String glob = PathUtils.toNestedDirGlob(path, nNestedDirs);
        Assert.assertEquals(glob, expectedGlob,
                String.format("Nested glob not match the expected value. path=%s, nNestedDirs=%d", path, nNestedDirs));
    }

    @Test(groups = "unit")
    private void testFileName() {
        Assert.assertEquals(PathUtils.getFileNameWithoutExtension("test.csv"), "test");
        Assert.assertEquals(PathUtils.getFileNameWithoutExtension("none"), "none");
        Assert.assertEquals(PathUtils.getFileNameWithoutExtension("with/folder"), "folder");
        Assert.assertEquals(PathUtils.getFileNameWithoutExtension("/with/multiple/folders.ext"), "folders");
        Assert.assertTrue(PathUtils.getFileNameWithoutExtension(".some").isEmpty());
        Assert.assertEquals(PathUtils.getFileNameWithoutExtension("multiple.extension.test"), "multiple.extension");
    }

    @DataProvider(name = "toNestedDirGlob")
    private Object[][] toNestedDirGlobTestData() {
        return new Object[][] { //
                { "/path/to/dir", 0, "/path/to/dir" }, //
                { "/path/to/dir", 1, "/path/to/dir/*" }, //
                { "/path/to/dir/test.avro", 1, "/path/to/dir/*" }, //
                { "/path/to/dir/test.parquet", 1, "/path/to/dir/*" }, //
                { "/path/to/dir", 4, "/path/to/dir/*/*/*/*" }, //
                { "/path/to/dir/", 4, "/path/to/dir/*/*/*/*" }, // remove trailing slash
        };
    }
}
