package com.latticeengines.spark.testframework;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.spark.service.impl.LivyServerManager;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-spark-context.xml" })
public abstract class SparkFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private LivyServerManager livyServerManager;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    protected String livyHost;

    protected void setupLivyHost() {
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = livyServerManager.getLivyHost();
        } else {
            livyHost = "http://localhost:8998";
        }
    }

}
