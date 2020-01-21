package com.latticeengines.datacloudapi.engine.testframework;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-engine-context.xml" })
public abstract class PropDataEngineAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final String SUCCESS_FLAG = "/_SUCCESS";

    @Value("${datacloud.test.env}")
    protected String testEnv;

    @Value("${datacloud.collection.sqoop.mapper.number}")
    private int numMappers;

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    @Inject
    protected Configuration yarnConfiguration;

    protected String podId;

    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        this.podId = podId;
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
