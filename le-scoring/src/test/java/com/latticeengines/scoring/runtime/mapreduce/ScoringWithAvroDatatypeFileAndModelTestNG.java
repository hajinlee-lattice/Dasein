package com.latticeengines.scoring.runtime.mapreduce;

import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

public class ScoringWithAvroDatatypeFileAndModelTestNG extends ScoringFunctionalTestNGBase {

    // !!! need to comment out the clearTables() in ScoringFunctionalTestNGBase
    // when using this test in QA environment !!!

    @Autowired
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    // need do change it according to the customer
    private static final String customer = "Nutanix_TEST_DELL";

    private String inputLeadsTable;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        // need to change the inputLeadsTable name to match the status in real
        // environments.
        inputLeadsTable = getClass().getSimpleName() + "__LeadsTable";
        CustomerSpace.parse(customer).toString();
    }

    @Test(groups = "functional", enabled = false)
    public void loadAndScore() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable,
                0, 7727482, new Timestamp(System.currentTimeMillis()));
        // set a fake Pid
        scoringCommand.setPid(1234L);
        // submit scoring job directly without going through database
        ApplicationId appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand, ScoringCommandStep.SCORE_DATA);
        waitForSuccess(appId, ScoringCommandStep.SCORE_DATA);
    }

}
