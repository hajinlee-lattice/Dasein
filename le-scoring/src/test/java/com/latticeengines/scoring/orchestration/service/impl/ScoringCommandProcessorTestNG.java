package com.latticeengines.scoring.orchestration.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.sql.Timestamp;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
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
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
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
        if(!CollectionUtils.isEmpty(metadataService.showTable(scoringJdbcTemplate, inputLeadsTable))){
            metadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
        }
        metadataService.createNewTableFromExistingOne(scoringJdbcTemplate, inputLeadsTable, testInputTable);
        tenant = CustomerSpace.parse(customer).toString();
        path = customerBaseDir + "/" + tenant + "/scoring";
        HdfsUtils.rmdir(yarnConfiguration, path);

        URL modelSummaryUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json"); //
        modelPath = customerBaseDir + "/" + tenant + "/models/" + inputLeadsTable
                + "/1e8e6c34-80ec-4f5b-b979-e79c8cc6bec3/1425511391553_3007";
        HdfsUtils.mkdir(yarnConfiguration, modelPath);
        String filePath = modelPath + "/1_model.json";
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
    public void testWorkflow() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable,
                0, 3891, new Timestamp(System.currentTimeMillis()));

        scoringCommandEntityMgr.create(scoringCommand);

        int iterations = 0;
        while ((scoringCommand.getStatus() == ScoringCommandStatus.POPULATED) && iterations < 100) {
            iterations++;
            Thread.sleep(15000);
            scoringCommand = scoringCommandEntityMgr.findByKey(scoringCommand);
        }

        ScoringCommandState state = scoringCommandStateEntityMgr.findByScoringCommandAndStep(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        ScoringCommandResult scoringCommandResult = scoringCommandResultEntityMgr.findByKey(state.getLeadOutputQueuePid());
        if (scoringCommandResult == null || scoringCommandResult.getStatus() == ScoringCommandStatus.NEW) {
            List<ScoringCommandLog> logs = scoringCommandLogEntityMgr.findAll();
            for (ScoringCommandLog scoringCommandLog : logs) {
                log.info(scoringCommandLog.getMessage());
            }
        }

        assertTrue(scoringCommandResult.getStatus() == ScoringCommandStatus.POPULATED, "The actual command state is "
                + scoringCommand.getStatus());

        List<ScoringCommandLog> logs = scoringCommandLogEntityMgr.findAll();
        assertTrue(logs.size() >= 12);

        List<ScoringCommandState> states = scoringCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 4);

        outputTable = scoringCommandResult.getTableName();
        assertEquals(metadataService.getRowCount(scoringJdbcTemplate, testInputTable),
                metadataService.getRowCount(scoringJdbcTemplate, outputTable));

    }
}
