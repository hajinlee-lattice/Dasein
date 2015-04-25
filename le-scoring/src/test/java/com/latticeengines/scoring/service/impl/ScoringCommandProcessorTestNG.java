package com.latticeengines.scoring.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.sql.Timestamp;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:scoring-scoringmanager-quartz-context.xml" })
public class ScoringCommandProcessorTestNG extends ScoringFunctionalTestNGBase {

    private static final String customer = "Nutanix";

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;
    
    @Autowired
    private ScoringCommandLogEntityMgr scoringCommandLogEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private DbCreds scoringCreds;

    private String outputTable;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        String path = customerBaseDir + "/" + customer + "/scoring";
        HdfsUtils.rmdir(yarnConfiguration, path);

        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/VisiDBTest_Model_Submission1_2015-04-18_12-30_model.json"); //
        String modelPath = customerBaseDir + "/" + customer + "/models/" + testInputTable
                + "/f3e29fd8-fb88-4758-9bcb-ea8d48ae6ac5/1429553747321_0004";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String filePath = modelPath + "/model.json";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelSummaryUrl.getFile(), filePath);
    }

    @AfterMethod(enabled = true, lastTimeOnly = true, alwaysRun = true)
    public void afterEachTest() {
        if (outputTable != null) {
            metadataService.dropTable(scoringJdbcTemplate, outputTable);
            clearTables();
        }
    }

    @Test(groups = "functional")
    public void testWorkflow() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, testInputTable, 0, 4352, new Timestamp(System.currentTimeMillis()));

        scoringCommandEntityMgr.create(scoringCommand);

        int iterations = 0;
        while ((scoringCommand.getStatus() == ScoringCommandStatus.POPULATED) && iterations < 100) { 
            iterations++;
            Thread.sleep(15000);
            scoringCommand = scoringCommandEntityMgr.findByKey(scoringCommand);
        }

        ScoringCommandResult scoringCommandResult = scoringCommandResultEntityMgr.findByScoringCommand(scoringCommand);
        if(scoringCommandResult == null || scoringCommandResult.getStatus() == ScoringCommandStatus.NEW) {
            List<ScoringCommandLog> logs = scoringCommandLogEntityMgr.findAll();
            for (ScoringCommandLog scoringCommandLog : logs) {
                log.info(scoringCommandLog.getMessage());
            }
        }

        assertTrue(scoringCommandResult.getStatus() == ScoringCommandStatus.POPULATED,
                "The actual command state is " + scoringCommand.getStatus());

        List<ScoringCommandLog> logs = scoringCommandLogEntityMgr.findAll();
        assertTrue(logs.size() >= 12);

        List<ScoringCommandState> states = scoringCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 4);

        outputTable = scoringCommandResult.getTableName();
        assertEquals(metadataService.getRowCount(scoringJdbcTemplate, testInputTable), metadataService.getRowCount(scoringJdbcTemplate, outputTable));

    }
}
