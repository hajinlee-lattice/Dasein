package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.annotations.Test;

public class HdfsUtilsUnitTestNG {
    
    private Configuration yarnConfiguration = new YarnConfiguration();

    @Test(groups = "unit")
    public void isDirectory() throws Exception {
        assertFalse(HdfsUtils.isDirectory(yarnConfiguration, "/tmp/*.avro"));
    }
}
