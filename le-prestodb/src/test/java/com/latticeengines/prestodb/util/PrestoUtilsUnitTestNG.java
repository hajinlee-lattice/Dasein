package com.latticeengines.prestodb.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PrestoUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testParseAvscPath() throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("presto-result/showCreateTable.sql");
        Assert.assertNotNull(is);
        String createStmt = IOUtils.toString(is, StandardCharsets.UTF_8);
        String avscPath = PrestoUtils.parseAvscPath(createStmt);
        Assert.assertEquals(avscPath, "hdfs://localhost:9000/presto-schema/avsc/TestTable.avsc");
    }

}
