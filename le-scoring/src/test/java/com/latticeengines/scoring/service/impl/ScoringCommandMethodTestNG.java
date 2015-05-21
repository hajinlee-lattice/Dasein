package com.latticeengines.scoring.service.impl;

import java.sql.Timestamp;

import javax.annotation.Resource;

import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.monitor.alerts.service.impl.BaseAlertServiceImpl;
import com.latticeengines.monitor.alerts.service.impl.PagerDutyTestUtils;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringCommandLogService;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ScoringCommandMethodTestNG extends ScoringFunctionalTestNGBase {

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private ScoringCommandLogService scoringCommandLogService;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Resource(name = "modelingAlertService")
    private BaseAlertServiceImpl alertService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;
    
    @Autowired
    private ScoringProcessorCallable scoringProcessor;

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Value("${scoring.output.table.sample}")
    private String testOutputTable;

    private static final double cleanUpInterval = 0.000001;

    private ScoringManagerServiceImpl scoringManager;

    private static final String inputTable = "some_table";

    private static final String outputTable = "some_output_table";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        scoringManager = new ScoringManagerServiceImpl();
        scoringManager.setCustomerBaseDir(customerBaseDir);
        scoringManager.setEnableCleanHdfs(true);
        scoringManager.setCleanUpInterval(cleanUpInterval);
        scoringManager.init(applicationContext);
        alertService.enableTestMode();
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

        metadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, inputTable, testInputTable);
        scoringCommand.setStatus(ScoringCommandStatus.POPULATED);
        scoringCommandEntityMgr.update(scoringCommand);
        scoringCommand = scoringCommandEntityMgr.findAll().get(0);
        ScoringCommandState scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.LOAD_DATA);
        scoringCommandStateEntityMgr.create(scoringCommandState);
        scoringManager.cleanTables();
        assertEquals(scoringCommandEntityMgr.findAll().size(), 1);
        assertEquals(scoringCommandStateEntityMgr.findAll().size(), 1);
        assertEquals(metadataService.showTable(scoringJdbcTemplate, inputTable).get(0), inputTable);

        scoringCommand = scoringCommandEntityMgr.findAll().get(0);
        scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.SCORE_DATA);
        scoringCommandStateEntityMgr.create(scoringCommandState);
        scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        scoringCommandStateEntityMgr.create(scoringCommandState);
        scoringCommandLogService.log(scoringCommand, "some logs");
        ScoringCommandResult scoringCommandResult = new ScoringCommandResult("Nutanix", ScoringCommandStatus.NEW,
                outputTable, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandResultEntityMgr.create(scoringCommandResult);
        scoringManager.cleanTables();
        assertEquals(scoringCommandResultEntityMgr.findAll().size(), 1);
        assertEquals(scoringCommandLogService.findByScoringCommand(scoringCommand).size(), 1);
        assertEquals(scoringCommandStateEntityMgr.findAll().size(), 3);

        metadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, outputTable, testOutputTable);
        assertEquals(metadataService.showTable(scoringJdbcTemplate, outputTable).get(0), outputTable);

        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommand.setConsumed(new Timestamp(System.currentTimeMillis()));
        scoringCommandResult.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandResult.setConsumed(new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.update(scoringCommand);
        scoringCommandResultEntityMgr.update(scoringCommandResult);
        Thread.sleep(4000);
        scoringManager.cleanTables();

        assertEquals(scoringCommandStateEntityMgr.findAll().size(), 0);
        assertEquals(scoringCommandLogService.findByScoringCommand(scoringCommand).size(), 0);
        assertNull(scoringCommandEntityMgr.findByKey(scoringCommand));
        assertNull(scoringCommandResultEntityMgr.findByKey(scoringCommandResult));
        assertEquals(metadataService.showTable(scoringJdbcTemplate, inputTable).size(), 0);
        assertEquals(metadataService.showTable(scoringJdbcTemplate, outputTable).size(), 0);
    }

    @Test(groups = "functional", enabled = true)
    public void testHandleJobFailed() throws ParseException {
        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.POPULATED, inputTable, 0,
                100, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);

        scoringCommandLogService.log(scoringCommand, "message.  #%#$%%^$%^$%^$%^");
        scoringCommandLogService.log(scoringCommand, "another message.  #%#$%%^$%^$%^$%^ 12344       .");

        scoringProcessor.setScoringCommand(scoringCommand);
        String failedAppId = "application_1415144508340_0729";
        ScoringCommandState scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.SCORE_DATA);
        scoringCommandStateEntityMgr.create(scoringCommandState);
        PagerDutyTestUtils.confirmPagerDutyIncident(scoringProcessor.handleJobFailed(failedAppId));
        PagerDutyTestUtils.confirmPagerDutyIncident(scoringProcessor.handleJobFailed());
    }

}
