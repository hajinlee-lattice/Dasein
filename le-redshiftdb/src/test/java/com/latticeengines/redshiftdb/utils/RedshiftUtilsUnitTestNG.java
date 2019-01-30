package com.latticeengines.redshiftdb.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.redshift.RedshiftUnloadParams;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

public class RedshiftUtilsUnitTestNG {

    private static final String TABLE_NAME = "TableName";
    private static final String S3_PATH = "s3://mybucket/myfolder";
    private static final String AUTHORIZATION = "authorization";

    @Test(groups = "unit")
    public void normal() {
        RedshiftUnloadParams unloader = new RedshiftUnloadParams();
        String statement = RedshiftUtils.unloadTableStatement(TABLE_NAME, S3_PATH, AUTHORIZATION, unloader);
        Assert.assertNotNull(statement);
        Assert.assertTrue(statement.contains("HEADER"), statement);
        Assert.assertTrue(statement.contains("DELIMITER AS ','"), statement);
        Assert.assertTrue(statement.contains("ESCAPE AS '\"'"), statement);
        Assert.assertFalse(statement.contains("GZIP"), statement);
    }

    @Test(groups = "unit")
    public void compress() {
        RedshiftUnloadParams unloader = new RedshiftUnloadParams();
        unloader.setCompress(true);
        String statement = RedshiftUtils.unloadTableStatement(TABLE_NAME, S3_PATH, AUTHORIZATION, unloader);
        Assert.assertNotNull(statement);
        Assert.assertTrue(statement.contains("HEADER"), statement);
        Assert.assertTrue(statement.contains("DELIMITER AS ','"), statement);
        Assert.assertTrue(statement.contains("ESCAPE AS '\"'"), statement);
        Assert.assertTrue(statement.contains("GZIP"), statement);
    }

    @Test(groups = "unit")
    public void noHeader() {
        RedshiftUnloadParams unloader = new RedshiftUnloadParams();
        unloader.setNoHeader(true);
        String statement = RedshiftUtils.unloadTableStatement(TABLE_NAME, S3_PATH, AUTHORIZATION, unloader);
        Assert.assertNotNull(statement);
        Assert.assertFalse(statement.contains("HEADER"), statement);
        Assert.assertTrue(statement.contains("DELIMITER AS ','"), statement);
        Assert.assertTrue(statement.contains("ESCAPE AS '\"'"), statement);
        Assert.assertFalse(statement.contains("GZIP"), statement);
    }

}
