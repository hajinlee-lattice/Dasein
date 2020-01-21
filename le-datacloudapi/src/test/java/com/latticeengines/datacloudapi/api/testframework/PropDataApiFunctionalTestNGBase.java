package com.latticeengines.datacloudapi.api.testframework;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;

public abstract class PropDataApiFunctionalTestNGBase extends PropDataApiAbstractTestNGBase {

    @Value("${propdata.api.functional.hostport}")
    private String hostPort;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private Configuration yarnConfiguration;

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
