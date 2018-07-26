package com.latticeengines.hadoop.bean;

import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.HdfsUtils;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-hadoop-context.xml" })
public class HadoopConfigurationTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private EMRService emrService;

    @Test(groups = "functional", enabled = false)
    public void testEmrYarnConfiguration() throws IOException {
        Assert.assertEquals(yarnConfiguration.get("fs.defaultFS"), //
                String.format("hdfs://%s", emrService.getMasterIp()));
        InputStream is = getResourceStream();
        String hdfsPath = "/tmp/yarn-config-test/test.txt";
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        }
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, hdfsPath);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, hdfsPath));
    }

    private InputStream getResourceStream() {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream("test.txt");
    }

}
