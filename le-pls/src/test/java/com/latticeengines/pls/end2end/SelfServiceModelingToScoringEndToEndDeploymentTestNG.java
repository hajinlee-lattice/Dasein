package com.latticeengines.pls.end2end;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

@Component
public class SelfServiceModelingToScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(SelfServiceModelingToScoringEndToEndDeploymentTestNG.class);
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @Autowired
    private ScoreCorrectnessService scoreCompareService;

    private Tenant tenant;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        tenant = selfServiceModeling.getTenant();
        log.info(tenant);
    }

    @Test(groups = "deployment.lp", enabled = true)
    public void testLeadModelToScoreCorrectness() throws InterruptedException, IOException {
        String fileName = "Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv";
        String modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceLead, fileName);
        scoreCompareService.analyzeScores(tenant.getId(), RESOURCE_BASE + "/" + fileName, modelId, 1000);
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void testAccountModelToScoreCorrectness() throws InterruptedException, IOException {
        String fileName = "Mulesoft_SFDC_LP3_ModelingAccount_20160412_3kRows.csv";
        String modelId = selfServiceModeling.prepareModel(SchemaInterpretation.SalesforceAccount, fileName);
        scoreCompareService.analyzeScores(tenant.getId(), RESOURCE_BASE + "/" + fileName, modelId, 100);
    }

}
