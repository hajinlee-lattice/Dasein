package com.latticeengines.pls.end2end;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class AnalyzeScoresFromPreGeneratedModelDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Log log  = LogFactory.getLog(AnalyzeScoresFromPreGeneratedModelDeploymentTestNG.class);

    private static final String TENANT_ID = "LETest1461982161361.LETest1461982161361.Production";
    private static final String MODEL_ID = "ms__2e42399e-571a-48e9-b98f-9b40b269ef04-SelfServ";

    private static final int NUM_RECORDS_TO_SCORE = 100;
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final String fileName = "Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv";

    @Value("${pls.scoringapi.rest.endpoint.hostport}")
    private String scoringApiHostPort;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreCorrectnessService scoreCorrectnessService;

    @Autowired
    private TenantService tenantService;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        Tenant tenant = tenantService.findByTenantId(TENANT_ID);
        if (tenant == null) {
            tenant = new Tenant();
            tenant.setId(TENANT_ID);
            tenant.setName(CustomerSpace.parse(TENANT_ID).getTenantId());
            tenantService.registerTenant(tenant);
        }
        tenant.setRegisteredTime(12345L);
        tenantService.updateTenant(tenant);

        log.info("wait 10 sec for model to be downloaded.");
        Thread.sleep(10000L);
    }

    @AfterClass(groups = "deployment.lp")
    public void teardown() throws Exception {
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void useLocalScoredTextAndCompareScores() throws InterruptedException, IOException {
        SSLUtils.turnOffSslChecking();
        String pathToModelInputCsv = RESOURCE_BASE + "/" + fileName;
        scoreCorrectnessService.analyzeScores(TENANT_ID, pathToModelInputCsv, MODEL_ID, NUM_RECORDS_TO_SCORE);
    }

}
