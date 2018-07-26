package com.latticeengines.aws.emr.impl;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.emr.EMRService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-aws-context.xml" })
public class EMRServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(EMRServiceImplTestNG.class);

    @Value("${aws.test.emr.cluster}")
    private String clusterName;

    @Autowired
    private EMRService emrService;

    @BeforeClass(groups = "functional")
    private void setup() {
    }

    @AfterClass(groups = "functional")
    private void teardown() {
    }

    @Test(groups = "functional")
    public void testExtractConfiguration() {
        String masterIp = emrService.getMasterIp(clusterName);
        Assert.assertTrue(StringUtils.isNotBlank(masterIp));
    }

}
