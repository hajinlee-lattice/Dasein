package com.latticeengines.api.functionalframework;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.dataplatform.entitymanager.impl.JobEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ModelEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ThrottleConfigurationEntityMgrImpl;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-api-context.xml" })
public class ApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired 
    private YarnClient defaultYarnClient;
    
    @Autowired
    private JobEntityMgrImpl jobEntityMgr;
    
    @Autowired
    private ModelEntityMgrImpl modelEntityMgr;
    
    @Autowired
    protected ThrottleConfigurationEntityMgrImpl throttleConfigurationEntityMgr;

    protected DataPlatformFunctionalTestNGBase platformTestBase;

    protected boolean doYarnClusterSetup() {
        return true;
    }

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        
        platformTestBase.setYarnClient(defaultYarnClient);
        platformTestBase.setJobEntityMgr(jobEntityMgr);
        platformTestBase.setModelEntityMgr(modelEntityMgr);
        platformTestBase.setThrottleConfigurationEntityMgr(throttleConfigurationEntityMgr);
        restTemplate.setErrorHandler(new DefaultResponseErrorHandler());
        if (!doYarnClusterSetup()) {
            return;
        }
        platformTestBase.setupRunEnvironment();
    }
    
    static class DefaultResponseErrorHandler implements ResponseErrorHandler {

        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            if (response.getStatusCode() == HttpStatus.OK) {
                return false;
            }
            return true;
        }

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            //String exceptionStack = StreamUtils.copyToString(response.getBody(), Charset.defaultCharset());
            //System.out.println(exceptionStack);
        }

    }

}