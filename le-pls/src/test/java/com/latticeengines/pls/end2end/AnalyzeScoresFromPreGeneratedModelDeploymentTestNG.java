package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class AnalyzeScoresFromPreGeneratedModelDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log  = LoggerFactory.getLogger(AnalyzeScoresFromPreGeneratedModelDeploymentTestNG.class);

    private static final String TENANT_ID = "LETest1477531073625.LETest1477531073625.Production";
    private static final String MODEL_ID = "ms__5368bf16-b745-4d91-9ffd-b913fa458015-SelfServ";

    private static final int NUM_RECORDS_TO_SCORE = 100;
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final String fileName = "Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv";

    @Value("${common.test.scoringapi.url}")
    private String scoringApiHostPort;

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
        tenant.setRegisteredTime(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(365));
        tenantService.updateTenant(tenant);

        log.info("wait 10 sec for model to be downloaded.");
        Thread.sleep(10000L);
    }

    @AfterClass(groups = "deployment.lp")
    public void teardown() throws Exception {
    }

    @Test(groups = "deployment.lp", enabled = true)
    public void useLocalScoredTextAndCompareScores() throws InterruptedException, IOException {
        String pathToModelInputCsv = RESOURCE_BASE + "/" + fileName;
        scoreCorrectnessService.analyzeScores(TENANT_ID, pathToModelInputCsv, MODEL_ID, NUM_RECORDS_TO_SCORE);
    }

}
