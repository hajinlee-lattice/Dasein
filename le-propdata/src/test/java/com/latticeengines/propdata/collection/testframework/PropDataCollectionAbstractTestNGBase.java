package com.latticeengines.propdata.collection.testframework;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.propdata.collection.service.impl.HdfsPathBuilder;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-collection-context.xml" })
public abstract class PropDataCollectionAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${propdata.test.env}")
    protected String testEnv;

    @Value("${propdata.collection.host}")
    private String dbHost;

    @Value("${propdata.collection.port}")
    private int dbPort;

    @Value("${propdata.collection.db}")
    private String db;

    @Value("${propdata.collection.user}")
    private String dbUser;

    @Value("${propdata.collection.password.encrypted}")
    private String dbPassword;

    @Value("${propdata.collection.sqoop.mapper.number}")
    private int numMappers;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

}
