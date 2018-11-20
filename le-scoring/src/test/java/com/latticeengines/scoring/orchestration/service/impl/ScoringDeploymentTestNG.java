package com.latticeengines.scoring.orchestration.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.sql.Timestamp;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@ContextConfiguration(locations = { "classpath:test-scoring-deployment-context.xml" })
public class ScoringDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(ScoringDeploymentTestNG.class);

    private final static String LEAD_INPUT_BASE_TABLE_NAME = "ScoringDeploymentTestNG_Base_LeadsTable";
    private final static String LEAD_INPUT_TABLE_NAME = "ScoringDeploymentTestNG_LeadsTable";

    // (YSong - M25) This is hacky, but since it is a retiring test, I think it is OK.
    private final static String QUARTZ_EMR = "quartz";

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private ScoringCommandLogEntityMgr scoringCommandLogEntityMgr;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private DbMetadataService dbMetadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Inject
    private EMREnvService emrEnvService;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

    @Value("${common.le.environment}")
    private String leEnv;

    private String outputTable;
    private String customer;
    private String tenant;
    private String path;
    private String modelDirectory;
    private ScoringCommand scoringCommand;
    private ScoringCommandResult scoringCommandResult;
    private Configuration emrConfiguration;

    public ScoringDeploymentTestNG() {
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        customer = getClass().getSimpleName();
        tenant = CustomerSpace.parse(customer).toString();
        if (!CollectionUtils.isEmpty(dbMetadataService.showTable(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME))) {
            dbMetadataService.dropTable(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME);
        }

        if (!CollectionUtils.isEmpty(dbMetadataService.showTable(scoringJdbcTemplate, LEAD_INPUT_BASE_TABLE_NAME))) {
            dbMetadataService.createNewTableFromExistingOne(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME,
                    LEAD_INPUT_BASE_TABLE_NAME);
        } else {
            throw new Exception("The lead input base table for scoringDeploymentTest does not exist.");
        }

        if (CollectionUtils.isEmpty(dbMetadataService.showTable(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME))) {
            throw new Exception("Could not find the lead input base table for scoringDeploymentTest: " + LEAD_INPUT_TABLE_NAME);
        }

        emrConfiguration = yarnConfiguration;
        if ("qacluster".equals(leEnv)) {
            emrConfiguration = emrEnvService.getYarnConfiguration(QUARTZ_EMR);
        }

        path = customerBaseDir + "/" + tenant + "/scoring";
        HdfsUtils.mkdir(yarnConfiguration, path);
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
        modelDirectory = customerBaseDir + "/" + tenant + "/models/" + LEAD_INPUT_TABLE_NAME
                + "/1e8e6c34-80ec-4f5b-b979-e79c8cc6bec3/1425511391553_3007";
        String modelPath = modelDirectory + "/" + "1_model.json";
        HdfsUtils.mkdir(emrConfiguration, modelDirectory);
        HdfsUtils.copyLocalToHdfs(emrConfiguration, modelSummaryUrl.getFile(), modelPath);
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void cleanup() throws Exception {
        if (outputTable != null) {
            dbMetadataService.dropTable(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME);
            dbMetadataService.dropTable(scoringJdbcTemplate, outputTable);
            // clean up the rows in four tables
            scoringCommandEntityMgr.delete(scoringCommand);
            scoringCommandLogEntityMgr.delete(scoringCommand);
            scoringCommandStateEntityMgr.delete(scoringCommand);
            scoringCommandResultEntityMgr.delete(scoringCommandResult);
        }
        HdfsUtils.rmdir(emrConfiguration, path);
        HdfsUtils.rmdir(emrConfiguration, modelDirectory);
    }

    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        // insert one row into the leadInputQueue
        scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, LEAD_INPUT_TABLE_NAME, 0, 3891,
                new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);
        log.info("Created scoring command with LeadInputQueue_ID=" + scoringCommand.getPid());

        // wait for the scoringManager to pick up the row in leadInputQueue
        int iterations = 0;
        while ((scoringCommand.getStatus() == ScoringCommandStatus.POPULATED) && iterations < 100) {
            iterations++;
            Thread.sleep(15000);
            scoringCommand = scoringCommandEntityMgr.findByKey(scoringCommand);
        }

        // get the information from scoring result table
        ScoringCommandState scoringCommandState = scoringCommandStateEntityMgr.findByScoringCommandAndStep(
                scoringCommand, ScoringCommandStep.EXPORT_DATA);
        Assert.assertNotNull(scoringCommandState,
                "Could not find a command state at step " + ScoringCommandStep.EXPORT_DATA
                        + " for scoring command id=" + scoringCommand.getId());
        scoringCommandResult = scoringCommandResultEntityMgr.findByKey(scoringCommandState.getLeadOutputQueuePid());
        if (scoringCommandResult == null || scoringCommandResult.getStatus() == ScoringCommandStatus.NEW) {
            List<ScoringCommandLog> scoringCommandLogs = scoringCommandLogEntityMgr.findAll();
            for (ScoringCommandLog scoringCommandLog : scoringCommandLogs) {
                log.info(scoringCommandLog.getMessage());
            }
        }

        assertTrue(scoringCommandResult.getStatus() == ScoringCommandStatus.POPULATED,
                "The actual scoring command status is " + scoringCommandResult.getStatus());

        assertTrue(scoringCommandLogEntityMgr.findByScoringCommand(scoringCommand).size() >= 12);
        log.info("The number of scoring command logs is "
                + scoringCommandLogEntityMgr.findByScoringCommand(scoringCommand).size());
        assertEquals(scoringCommandStateEntityMgr.findByScoringCommand(scoringCommand).size(), 4);
        log.info("The number of scoring command states is is "
                + scoringCommandStateEntityMgr.findByScoringCommand(scoringCommand).size());

        outputTable = scoringCommandEntityMgr.findByKey(scoringCommand).getTableName();
        assertEquals(dbMetadataService.getRowCount(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME),
                dbMetadataService.getRowCount(scoringJdbcTemplate, outputTable));

    }
}
