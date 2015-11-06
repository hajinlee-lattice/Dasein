package com.latticeengines.scoring.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
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

@ContextConfiguration(locations = { "classpath:test-scoring-deployment-context.xml" })
public class ScoringDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(ScoringDeploymentTestNG.class);

    private final static String LEAD_INPUT_BASE_TABLE_NAME = "ScoringDeploymentTestNG_Base_LeadsTable";
    private final static String LEAD_INPUT_TABLE_NAME = "ScoringDeploymentTestNG_LeadsTable";

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
    private MetadataService metadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

    private String outputTable;
    private String customer;
    private String tenant;
    private String path;
    private String modelDirectory;
    private ScoringCommand scoringCommand;
    private ScoringCommandResult scoringCommandResult;

    public ScoringDeploymentTestNG() {
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        customer = getClass().getSimpleName().toString();
        tenant = CustomerSpace.parse(customer).toString();
        if (!CollectionUtils.isEmpty(metadataService.showTable(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME))) {
            metadataService.dropTable(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME);
        }

        if (!CollectionUtils.isEmpty(metadataService.showTable(scoringJdbcTemplate, LEAD_INPUT_BASE_TABLE_NAME))) {
            metadataService.createNewTableFromExistingOne(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME,
                    LEAD_INPUT_BASE_TABLE_NAME);
        } else {
            throw new Exception("The lead input base table for scoringDeploymentTest does not exist.");
        }

        path = customerBaseDir + "/" + tenant + "/scoring";
        HdfsUtils.mkdir(yarnConfiguration, path);
        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
        modelDirectory = customerBaseDir + "/" + tenant + "/models/" + LEAD_INPUT_TABLE_NAME
                + "/1e8e6c34-80ec-4f5b-b979-e79c8cc6bec3/1425511391553_3007";
        String modelPath = modelDirectory + "/" + "1_model.json";
        HdfsUtils.mkdir(yarnConfiguration, modelDirectory);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), modelPath);
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void cleanup() throws Exception {
        if (outputTable != null) {
            metadataService.dropTable(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME);
            metadataService.dropTable(scoringJdbcTemplate, outputTable);
            // clean up the rows in four tables
            scoringCommandEntityMgr.delete(scoringCommand);
            scoringCommandLogEntityMgr.delete(scoringCommand);
            scoringCommandStateEntityMgr.delete(scoringCommand);
            scoringCommandResultEntityMgr.delete(scoringCommandResult);
        }
        HdfsUtils.rmdir(yarnConfiguration, path);
        HdfsUtils.rmdir(yarnConfiguration, modelDirectory);
    }

    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        // insert one row into the leadInputQueue
        scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, LEAD_INPUT_TABLE_NAME, 0, 3891,
                new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);

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
        assertEquals(metadataService.getRowCount(scoringJdbcTemplate, LEAD_INPUT_TABLE_NAME),
                metadataService.getRowCount(scoringJdbcTemplate, outputTable));

    }
}
