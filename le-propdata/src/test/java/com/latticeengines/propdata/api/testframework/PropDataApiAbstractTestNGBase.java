package com.latticeengines.propdata.api.testframework;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.core.source.Source;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-api-context.xml" })
public abstract class PropDataApiAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${propdata.test.match.client}")
    protected String testMatchClientName;

    @Value("${propdata.test.env}")
    protected String testEnv;

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

    protected static <T> T sendHttpDeleteForObject(RestTemplate restTemplate, String url, Class<T> responseType) {
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.DELETE, jsonRequestEntity(""), responseType);
        return response.getBody();
    }

    protected static <T> T sendHttpPutForObject(RestTemplate restTemplate, String url, Object payload,
            Class<T> responseType) {
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.PUT, jsonRequestEntity(payload),
                responseType);
        return response.getBody();
    }

    protected static HttpEntity<String> jsonRequestEntity(Object payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        return new HttpEntity<>(JsonUtils.serialize(payload), headers);
    }


    protected void prepareCleanPod(String podId) {
        HdfsPodContext.changeHdfsPodId(podId);
        try {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    protected void uploadSourceAtVersion(Source source, String version) {
        InputStream baseAvroStream = ClassLoader
                .getSystemResourceAsStream("sources/" + source.getSourceName() + ".avro");
        String targetPath = hdfsPathBuilder.constructSnapshotDir(source, version).append("part-0000.avro")
                .toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = hdfsPathBuilder.constructSnapshotDir(source, version).append("_SUCCESS")
                    .toString();
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected MatchClient getMatchClient() {
        return MatchClient.valueOf(testMatchClientName);
    }

    abstract protected String getRestAPIHostPort();
}
