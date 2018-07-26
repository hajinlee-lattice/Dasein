package com.latticeengines.yarn.exposed.bean;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.aws.emr.EMRService;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-local-yarn-context.xml" })
public class MapReduceConfigurationTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private Configuration hadoopConfiguration;

    @Inject
    private EMRService emrService;

    @Test(groups = "functional", enabled = false)
    public void testEmrYarnConfiguration() throws IOException {
        Assert.assertEquals(hadoopConfiguration.get("fs.defaultFS"), //
                String.format("hdfs://%s", emrService.getMasterIp()));
    }

}
