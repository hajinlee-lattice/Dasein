package com.latticeengines.propdata.api.testframework;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;

public abstract class PropDataApiFunctionalTestNGBase extends PropDataApiAbstractTestNGBase {

    protected RestTemplate restTemplate = new RestTemplate();

    @Value("${propdata.api.functional.hostport}")
    private String hostPort;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private Configuration yarnConfiguration;

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    protected void prepareCleanPod(String podId) {
        hdfsPathBuilder.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
