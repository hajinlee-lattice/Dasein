package com.latticeengines.matchapi.testframework;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;

public class MatchapiFunctionalTestNGBase extends MatchapiAbstractTestNGBase {

    private HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory(
            HttpClientBuilder.create().build());
    protected RestTemplate restTemplate = new RestTemplate(clientHttpRequestFactory);

    @Value("${matchapi.test.functional.hostport}")
    private String hostPort;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
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
