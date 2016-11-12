package com.latticeengines.modelquality.service.impl;

import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.Environment;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunEntityNames;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.service.ModelRunService;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;

public abstract class AbstractModelRunServiceImpl implements ModelRunService {

    private static final Log log = LogFactory.getLog(AbstractModelRunServiceImpl.class);

    @Autowired
    private ModelRunEntityMgr modelRunEntityMgr;

    @Autowired
    private DataSetEntityMgr dataSetEntityMgr;

    @Autowired
    private AnalyticPipelineEntityMgr analyticPipelineEntityMgr;

    @Autowired
    protected MetricService metricService;

    protected Environment env;
    protected RestTemplate restTemplate;
    protected AuthorizationHeaderHttpRequestInterceptor authHeaderInterceptor = new AuthorizationHeaderHttpRequestInterceptor(
            "");

    @Override
    public void setEnvironment(Environment env) {
        this.env = env;
    }

    @Override
    public ModelRun createModelRun(ModelRunEntityNames modelRunEntityNames, Environment env) {
        this.env = env;
        ModelRun modelRun = new ModelRun();
        AnalyticPipeline analyticPipeline = analyticPipelineEntityMgr
                .findByName(modelRunEntityNames.getAnalyticPipelineName());
        DataSet dataset = dataSetEntityMgr.findByName(modelRunEntityNames.getDataSetName());
        modelRun.setName(modelRunEntityNames.getName());
        modelRun.setAnalyticPipeline(analyticPipeline);
        modelRun.setDataSet(dataset);
        modelRun.setStatus(ModelRunStatus.PROGRESS);
        modelRun.setDescription(modelRunEntityNames.getDescription());
        modelRun.setAnalyticTestName(modelRunEntityNames.getAnalyticTestName());
        modelRun.setAnalyticTestTag(modelRunEntityNames.getAnalyticTestTag());
        modelRunEntityMgr.create(modelRun);
        runAsync(modelRun);
        return modelRun;
    }

    private void runAsync(ModelRun modelRun) {
        Runnable runnable = new ModelRunRunnable(modelRun);
        Thread runner = new Thread(runnable);
        runner.start();
    }

    protected abstract void runModel(ModelRun modelRun);

    protected String getDeployedRestAPIHostPort() {
        String deployedHostPort = env.apiHostPort;
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    private void setup() throws Exception {
        restTemplate = HttpClientUtils.newRestTemplate();
        Tenant tenant = new Tenant();
        tenant.setId(env.tenant);
        tenant.setName(CustomerSpace.parse(env.tenant).getTenantId());
        loginAndAttach(env.username, env.password, tenant);
    }

    protected UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));
        String deployedHostPort = getDeployedRestAPIHostPort();

        LoginDocument doc = restTemplate.postForObject(deployedHostPort + "/pls/login", creds, LoginDocument.class);
        authHeaderInterceptor.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { authHeaderInterceptor }));

        UserDocument userDocument = restTemplate.postForObject(deployedHostPort + "/pls/attach", tenant,
                UserDocument.class);
        log.info("Log in user " + username + " to tenant " + tenant.getId() + " through REST call.");
        return userDocument;
    }

    protected void cleanup() {
        try {
            String deployedHostPort = getDeployedRestAPIHostPort();
            restTemplate.getForObject(deployedHostPort + "/pls/logout", Object.class);
        } catch (Exception ex) {
            log.warn("Failed to logout!", ex);
        }
    }

    private class ModelRunRunnable implements Runnable {
        private ModelRun modelRun;

        public ModelRunRunnable(ModelRun modelRun) {
            this.modelRun = modelRun;
        }

        @Override
        public void run() {
            try {
                setup();
                runModel(modelRun);
                modelRun.setStatus(ModelRunStatus.COMPLETED);
                modelRunEntityMgr.update(modelRun);

            } catch (Exception ex) {
                modelRun.setStatus(ModelRunStatus.FAILED);
                modelRun.setErrorMessage(ex.getMessage());
                modelRunEntityMgr.update(modelRun);
                log.error("Failed!", ex);

            } finally {
                cleanup();
            }
        }

    }
}
