package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class AnalyzeScoresFromPreGeneratedModelDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log  = LoggerFactory.getLogger(AnalyzeScoresFromPreGeneratedModelDeploymentTestNG.class);

    private static final String TENANT_ID = "LETest1602385214724.LETest1602385214724.Production";
    private static final String MODEL_ID = "ms__e2715049-e846-4021-a3e2-420affca922c-SelfServ";

    private static final int NUM_RECORDS_TO_SCORE = 10;
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final String fileName = "Hootsuite_PLS132_LP3_ScoringLead_20160330_165806_modified.csv";

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    protected ScoreCorrectnessService scoreCompareService;

    @Inject
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

        modelSummaryProxy.downloadModelSummary(TENANT_ID);
//        log.info("wait 10 sec for model to be downloaded.");
//        Thread.sleep(10000L);
    }

    @AfterClass(groups = "deployment.lp")
    public void teardown() throws Exception {
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void useLocalScoredTextAndCompareScores() throws InterruptedException, IOException {
        String pathToModelInputCsv = RESOURCE_BASE + "/" + fileName;
//        scoreCorrectnessService.analyzeScores(TENANT_ID, pathToModelInputCsv, MODEL_ID, NUM_RECORDS_TO_SCORE);
        Map<String, ComparedRecord> diffRecords = scoreCompareService.analyzeScores(TENANT_ID,
                pathToModelInputCsv, MODEL_ID, NUM_RECORDS_TO_SCORE);
        checkExpectedDifferentCount(diffRecords);
    }

    private void checkExpectedDifferentCount(Map<String, ComparedRecord> diffRecords) {
        log.info(String.format("diffRecords.size() is %d.", diffRecords.size()));
        String expectedDiffCountStr = System.getProperty("DIFFCOUNT");

        if (StringUtils.isNotBlank(expectedDiffCountStr)) {
            int expectedDiffCount = Integer.valueOf(expectedDiffCountStr);
            log.info("Checking if expected diff count is equal to " + expectedDiffCountStr);
            assertTrue(Math.abs(diffRecords.size() - expectedDiffCount) <= 2, //
                    "Got actual diff: " + diffRecords.size() //
                            +" which is too far away from expected diff count " + expectedDiffCountStr);
        } else {
            log.info("Property DIFFCOUNT not set.");
        }
    }

}
