package com.latticeengines.api.functionalframework;

import java.io.IOException;
import java.nio.charset.Charset;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.api.exposed.exception.ModelingServiceRestException;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.yarn.exposed.service.AwsBatchJobService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-api-context.xml" })
public class ApiFunctionalTestNGBase extends DataPlatformFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ApiFunctionalTestNGBase.class);

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    protected RestTemplate ignoreErrorRestTemplate = HttpClientUtils.newRestTemplate();

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private YarnClient defaultYarnClient;

    protected DataPlatformFunctionalTestNGBase platformTestBase;

    @Value("${api.rest.endpoint.hostport}")
    protected String restEndpointHost;

    @Resource(name = "awsBatchjobService")
    protected AwsBatchJobService awsBatchJobService;

    @Override
    public boolean doClearDbTables() {
        return false;
    }

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() {
        restTemplate.setErrorHandler(new ThrowExceptionResponseErrorHandler());
        ignoreErrorRestTemplate.setErrorHandler(new IgnoreErrorResponseErrorHandler());
        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);

        platformTestBase.setYarnClient(defaultYarnClient);
        platformTestBase.setAwsBatchJobService(awsBatchJobService);

    }

    static class ThrowExceptionResponseErrorHandler implements ResponseErrorHandler {

        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            if (response.getStatusCode() == HttpStatus.OK) {
                return false;
            }
            return true;
        }

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            String responseBody = IOUtils.toString(response.getBody(), Charset.defaultCharset());
            log.info("Error response from rest call: " + response.getStatusCode() + " " + response.getStatusText()
                    + " " + responseBody);

            throw new ModelingServiceRestException(responseBody);
        }
    }

    static class IgnoreErrorResponseErrorHandler implements ResponseErrorHandler {

        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            return response.getStatusCode() != HttpStatus.OK;
        }

        @Override
        public void handleError(ClientHttpResponse response) {
        }
    }

}
