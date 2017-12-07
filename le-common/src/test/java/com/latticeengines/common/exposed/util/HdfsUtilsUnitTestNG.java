package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class HdfsUtilsUnitTestNG {

    private Configuration yarnConfiguration = new YarnConfiguration();

    @Test(groups = "unit")
    public void isDirectory() throws Exception {
        assertFalse(HdfsUtils.isDirectory(yarnConfiguration, "/tmp/*.avro"));
    }

    @Test(groups = "unit", enabled = false)
    public void testDistCp() throws Exception {
        List<Pair<String, Class<?>>> columns = ImmutableList.of( //
                Pair.of("Id", Integer.class), //
                Pair.of("Value", String.class)
        );
        Object[][] data = new Object[][]{
                { 1, "1" }, //
                { 2, "2" }, //
                { 3, "3" },
        };
        String srcDir = "/tmp/HdfsUtilsTest/input";
        AvroUtils.uploadAvro(yarnConfiguration, data, columns, "test", srcDir);
        assertTrue(HdfsUtils.isDirectory(yarnConfiguration, srcDir));

        String tgtDir = "/tmp/HdfsUtilsTest/output";
        if (HdfsUtils.fileExists(yarnConfiguration, tgtDir)) {
            HdfsUtils.rmdir(yarnConfiguration, tgtDir);
        }
        assertFalse(HdfsUtils.isDirectory(yarnConfiguration, tgtDir));

        HdfsUtils.distcp(yarnConfiguration, srcDir, tgtDir, "default");
        assertTrue(HdfsUtils.isDirectory(yarnConfiguration, tgtDir));

        AvroUtils.iterator(yarnConfiguration, tgtDir + "/*.avro").forEachRemaining(System.out::println);
    }
}
