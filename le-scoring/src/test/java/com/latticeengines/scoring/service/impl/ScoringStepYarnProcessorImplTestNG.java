package com.latticeengines.scoring.service.impl;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

public class ScoringStepYarnProcessorImplTestNG extends ScoringFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ScoringStepYarnProcessorImplTestNG.class);

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    private static final String customer = "Nutanix";

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private DbCreds scoringCreds;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    private String outputTable;

    private String inputLeadsTable;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        inputLeadsTable = getClass().getSimpleName() + "_LeadsTable";
        metadataService.createNewTableFromExistingOne(scoringJdbcTemplate, inputLeadsTable, testInputTable);

        String path = customerBaseDir + "/" + customer + "/scoring";
        HdfsUtils.rmdir(yarnConfiguration, path);

        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/VisiDBTest_Model_Submission1_2015-04-18_12-30_model.json"); //
        String modelPath = customerBaseDir + "/" + customer + "/models/" + inputLeadsTable
                + "/f3e29fd8-fb88-4758-9bcb-ea8d48ae6ac5/1429553747321_0004";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String filePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), filePath);
        metadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
        metadataService.dropTable(scoringJdbcTemplate, outputTable);
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
        if (outputTable != null) {
            metadataService.dropTable(scoringJdbcTemplate, outputTable);
            metadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
            clearTables();
        }
    }

    @Test(groups = "functional")
    public void executeYarnSteps() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable, 0, 4352,
                new Timestamp(System.currentTimeMillis()));

        ApplicationId appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand.getId(),
                ScoringCommandStep.LOAD_DATA, scoringCommand);
        waitForSuccess(appId, ScoringCommandStep.LOAD_DATA);

        appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand.getId(), ScoringCommandStep.SCORE_DATA,
                scoringCommand);
        waitForSuccess(appId, ScoringCommandStep.SCORE_DATA);

        HdfsUtils.rmdir(yarnConfiguration, customerBaseDir + "/" + customer + "/scoring/" + inputLeadsTable +"/data/datatype.avsc");
        appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand.getId(), ScoringCommandStep.EXPORT_DATA,
                scoringCommand);
        waitForSuccess(appId, ScoringCommandStep.EXPORT_DATA);

        ScoringCommandResult scoringCommandResult = scoringCommandResultEntityMgr.findByScoringCommand(scoringCommand);
        outputTable = scoringCommandResult.getTableName();
        assertEquals(metadataService.getRowCount(scoringJdbcTemplate, testInputTable), metadataService.getRowCount(scoringJdbcTemplate, outputTable));
    }

    private void waitForSuccess(ApplicationId appId, ScoringCommandStep step) throws Exception {
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        log.info(step + ": appId succeeded: " + appId.toString());
    }
}
