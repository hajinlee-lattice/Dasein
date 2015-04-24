package com.latticeengines.scoring.service.impl;

import java.sql.Timestamp;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.dataplatform.service.impl.PagerDutyTestUtils;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringCommandLogService;
import com.latticeengines.scoring.service.ScoringStepProcessor;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ScoringCommandMethodTestNG extends ScoringFunctionalTestNGBase {

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandLogService scoringCommandLogService;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private ScoringStepYarnProcessor scoringStepYarnProcessor;

    @Autowired
    private ScoringStepProcessor scoringStepFinishProcessor;

    @Autowired
    private JobService jobService;

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private DbCreds scoringCreds;

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Value("${scoring.output.table.sample}")
    private String testOutputTable;

    private String appTimeLineWebAppAddress = "";

    private static final String cleanUpInterval = "0.000001";

    private ScoringManagerServiceImpl scoringManager;

    private static final String inputTable = "some_table";

    private static final String outputTable = "some_output_table";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        scoringManager = new ScoringManagerServiceImpl();
        scoringManager.setCleanUpInterval(cleanUpInterval);
        scoringManager.setScoringCommandEntityMgr(scoringCommandEntityMgr);
        scoringManager.setScoringCommandResultEntityMgr(scoringCommandResultEntityMgr);
        scoringManager.setScoringCommandStateEntityMgr(scoringCommandStateEntityMgr);
        scoringManager.setScoringCommandLogService(scoringCommandLogService);
        scoringManager.setSqoopSyncJobService(sqoopSyncJobService);
        scoringManager.setMetadataService(metadataService);
        scoringManager.setScoringCreds(scoringCreds);
        scoringManager.setScoringJdbcTemplate(scoringJdbcTemplate);
    }

    @Test(groups = "functional")
    public void testCleanTables() throws ParseException, NumberFormatException, InterruptedException {
        assertEquals(scoringCommandEntityMgr.findAll().size(), 0);
        assertEquals(scoringCommandResultEntityMgr.findAll().size(), 0);
        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.NEW, inputTable, 0, 100,
                new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);
        scoringManager.cleanTables();
        assertEquals(scoringCommandEntityMgr.findAll().size(), 1);

        sqoopSyncJobService.eval(metadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, inputTable, testInputTable), //
                "", "create-table", 1, metadataService.getJdbcConnectionUrl(scoringCreds));
        scoringCommand.setStatus(ScoringCommandStatus.POPULATED);
        scoringCommandEntityMgr.update(scoringCommand);
        scoringManager.cleanTables();
        assertEquals(scoringCommandEntityMgr.findAll().size(), 1);
        assertEquals(scoringJdbcTemplate.queryForList(metadataService.showTable(scoringJdbcTemplate, inputTable), String.class).get(0),
                inputTable);

        ScoringCommandResult scoringCommandResult = new ScoringCommandResult("Nutanix", ScoringCommandStatus.NEW, outputTable, 100,
                new Timestamp(System.currentTimeMillis()));
        scoringCommandResultEntityMgr.create(scoringCommandResult);
        scoringManager.cleanTables();
        assertEquals(scoringCommandResultEntityMgr.findAll().size(), 1);

        sqoopSyncJobService.eval( metadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, outputTable, testOutputTable), //
                "", "create-table", 1, metadataService.getJdbcConnectionUrl(scoringCreds));
        assertEquals(scoringJdbcTemplate.queryForList(metadataService.showTable(scoringJdbcTemplate, outputTable), String.class).get(0),
                outputTable);

        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommand.setConsumed(new Timestamp(System.currentTimeMillis()));
        scoringCommandResult.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandResult.setConsumed(new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.update(scoringCommand);
        scoringCommandResultEntityMgr.update(scoringCommandResult);
        Thread.sleep(4000);
        scoringManager.cleanTables();
        
        assertNull(scoringCommandEntityMgr.findByKey(scoringCommand));
        assertNull(scoringCommandResultEntityMgr.findByKey(scoringCommandResult));
        assertEquals(scoringJdbcTemplate.queryForList(metadataService.showTable(scoringJdbcTemplate, inputTable), String.class).size(), 0);
        assertEquals(scoringJdbcTemplate.queryForList(metadataService.showTable(scoringJdbcTemplate, outputTable), String.class).size(), 0);
    }

    @Test(groups = "functional", enabled = false)
    public void testHandleJobFailed() throws ParseException {
        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.POPULATED, "some_table", 0,
                100, new Timestamp(System.currentTimeMillis()));

        scoringCommandEntityMgr.create(scoringCommand);

        scoringCommandLogService.log(scoringCommand, "message.  #%#$%%^$%^$%^$%^");
        scoringCommandLogService.log(scoringCommand, "another message.  #%#$%%^$%^$%^$%^ 12344       .");

        ScoringProcessorCallable callable = new ScoringProcessorCallable(scoringCommand, scoringCommandEntityMgr,
                scoringCommandLogService, scoringCommandStateEntityMgr, scoringStepYarnProcessor,
                scoringStepFinishProcessor, jobService, yarnConfiguration, appTimeLineWebAppAddress);
        String failedAppId = "application_1415144508340_0729";
        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed(failedAppId));

        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed());
    }

}
