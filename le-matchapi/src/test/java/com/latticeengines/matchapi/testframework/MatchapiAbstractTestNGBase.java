package com.latticeengines.matchapi.testframework;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-matchapi-context.xml" })
public abstract class MatchapiAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    private MetricService metricService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private Configuration yarnConfiguration;

    @PostConstruct
    private void postConstruct() {
        metricService.disable();
    }

    protected abstract String getRestAPIHostPort();

    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
