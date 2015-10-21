package com.latticeengines.scoring.service.impl;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
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
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

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

    private String modelPath;

    private String path;

    private static String tenant;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        inputLeadsTable = getClass().getSimpleName() + "_LeadsTable";
        if (!CollectionUtils.isEmpty(metadataService.showTable(scoringJdbcTemplate, inputLeadsTable))) {
            metadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
        }

        metadataService.createNewTableFromExistingOne(scoringJdbcTemplate, inputLeadsTable, testInputTable);
        tenant = CustomerSpace.parse(customer).toString();
        path = customerBaseDir + "/" + tenant + "/scoring";
        HdfsUtils.rmdir(yarnConfiguration, path);

        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json"); //
        modelPath = customerBaseDir + "/" + tenant + "/models/" + inputLeadsTable
                + "/1e8e6c34-80ec-4f5b-b979-e79c8cc6bec3/1429553747321_0004";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String filePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), filePath);
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
        if (outputTable != null) {
            metadataService.dropTable(scoringJdbcTemplate, outputTable);
            metadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
            clearTables();
        }
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
            HdfsUtils.rmdir(yarnConfiguration, modelPath);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Test(groups = "functional")
    public void executeYarnSteps() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable,
                0, 4352, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);

        ApplicationId appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand, ScoringCommandStep.LOAD_DATA);
        waitForSuccess(appId, ScoringCommandStep.LOAD_DATA);

        appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand, ScoringCommandStep.SCORE_DATA);
        waitForSuccess(appId, ScoringCommandStep.SCORE_DATA);

        HdfsUtils.rmdir(yarnConfiguration, customerBaseDir + "/" + tenant + "/scoring/" + inputLeadsTable
                + "/data/datatype.avsc");
        ScoringCommandState state = new ScoringCommandState(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        scoringCommandStateEntityMgr.create(state);
        appId = scoringStepYarnProcessor.executeYarnStep(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        waitForSuccess(appId, ScoringCommandStep.EXPORT_DATA);

        state = scoringCommandStateEntityMgr
                .findByScoringCommandAndStep(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        ScoringCommandResult scoringCommandResult = scoringCommandResultEntityMgr.findByKey(state
                .getLeadOutputQueuePid());
        outputTable = scoringCommandResult.getTableName();
        assertEquals(metadataService.getRowCount(scoringJdbcTemplate, testInputTable),
                metadataService.getRowCount(scoringJdbcTemplate, outputTable));
    }

    @Test(groups = "functional", enabled = false)
    public void findModelUrlsToLocalize() throws Exception {
        
        //((ScoringStepYarnProcessorImpl)scoringStepYarnProcessor).findModelUrlsToLocalize(tenant, modelGuids, modelFilePaths)
        
    }
}
