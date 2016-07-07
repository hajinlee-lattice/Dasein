package com.latticeengines.modelquality.service.impl;

import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ModelRunStatus;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.service.ModelRunService;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.security.exposed.AuthorizationHeaderHttpRequestInterceptor;

public abstract class AbstractModelRunServiceImpl implements ModelRunService {

    private static final Log log = LogFactory.getLog(AbstractModelRunServiceImpl.class);

    @Value("${modelquality.admin.user}")
    private String adminUser;
    @Value("${modelquality.admin.password.encrypted}")
    private String adminPassword;
    @Value("${pls.test.deployment.api}")
    private String deployedHostPort;

    @Autowired
    private ModelRunEntityMgr modelRunEntityMgr;

    @Autowired
    protected MetricService metricService;

    protected RestTemplate restTemplate;
    protected AuthorizationHeaderHttpRequestInterceptor authHeaderInterceptor = new AuthorizationHeaderHttpRequestInterceptor(
            "");

    @Override
    public void run(ModelRun modelRun) {

        try {
            SelectedConfig config = modelRun.getSelectedConfig();
            setup(config);
            modelRunEntityMgr.create(modelRun);

            runModel(config);

            modelRun.setStatus(ModelRunStatus.COMPLETED);
            modelRunEntityMgr.update(modelRun);

        } catch (Exception ex) {
            modelRun.setStatus(ModelRunStatus.FAILED);
            System.out.println(ex.getMessage());
            modelRun.setErrorMessage(ex.getMessage());
            modelRunEntityMgr.update(modelRun);
            log.error("Failed!", ex);
            throw new RuntimeException(ex);

        } finally {
            cleanup();
        }
    }

    protected abstract void runModel(SelectedConfig config);

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    private void setup(SelectedConfig config) throws Exception {
        restTemplate = new RestTemplate();
        loginAndAttach(adminUser, adminPassword, config.getDataSet().getTenant());
    }

    protected UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

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
            restTemplate.getForObject(deployedHostPort + "/pls/logout", Object.class);
        } catch (Exception ex) {
            log.warn("Failed to logout!", ex);
        }
    }
}
