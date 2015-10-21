package com.latticeengines.scoring.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.sql.Timestamp;

import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.monitor.alerts.service.impl.AlertServiceImpl;
import com.latticeengines.monitor.alerts.service.impl.PagerDutyTestUtils;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.service.ScoringCommandLogService;
import com.latticeengines.scoring.service.ScoringDaemonService;

public class ScoringCommandMethodTestNG extends ScoringFunctionalTestNGBase {

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private AlertService alertService;

    @Autowired
    private ScoringCommandLogService scoringCommandLogService;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

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
        this.scoringManager = new ScoringManagerServiceImpl();
        this.scoringManager.setCustomerBaseDir(this.customerBaseDir);
        this.scoringManager.setEnableCleanHdfs(true);
        this.scoringManager.setCleanUpInterval(cleanUpInterval);
        this.scoringManager.init(this.applicationContext);
        ((AlertServiceImpl) this.alertService).enableTestMode();
        this.scoringProcessor.setAlertService(this.alertService);
        if (!CollectionUtils.isEmpty(this.metadataService.showTable(this.scoringJdbcTemplate, inputTable))) {
            this.metadataService.dropTable(this.scoringJdbcTemplate, inputTable);
        }
        if (!CollectionUtils.isEmpty(this.metadataService.showTable(this.scoringJdbcTemplate, outputTable))) {
            this.metadataService.dropTable(this.scoringJdbcTemplate, outputTable);
        }
    }

    @Test(groups = "functional")
    public void testCleanTables() throws ParseException, NumberFormatException, InterruptedException {
        assertEquals(this.scoringCommandEntityMgr.findAll().size(), 0);
        assertEquals(this.scoringCommandResultEntityMgr.findAll().size(), 0);
        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.NEW, inputTable, 0, 100,
                new Timestamp(System.currentTimeMillis()));
        this.scoringCommandEntityMgr.create(scoringCommand);
        this.scoringManager.cleanTables();
        assertEquals(this.scoringCommandEntityMgr.findAll().size(), 1);

        this.metadataService.createNewEmptyTableFromExistingOne(this.scoringJdbcTemplate, inputTable,
                this.testInputTable);
        scoringCommand.setStatus(ScoringCommandStatus.POPULATED);
        this.scoringCommandEntityMgr.update(scoringCommand);
        scoringCommand = this.scoringCommandEntityMgr.findAll().get(0);
        ScoringCommandState scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.LOAD_DATA);
        this.scoringCommandStateEntityMgr.create(scoringCommandState);
        this.scoringManager.cleanTables();
        assertEquals(this.scoringCommandEntityMgr.findAll().size(), 1);
        assertEquals(this.scoringCommandStateEntityMgr.findAll().size(), 1);
        assertEquals(this.metadataService.showTable(this.scoringJdbcTemplate, inputTable).get(0), inputTable);

        scoringCommand = this.scoringCommandEntityMgr.findAll().get(0);
        scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.SCORE_DATA);
        this.scoringCommandStateEntityMgr.create(scoringCommandState);
        scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        this.scoringCommandStateEntityMgr.create(scoringCommandState);
        this.scoringCommandLogService.log(scoringCommand, "some logs");
        ScoringCommandResult scoringCommandResult = new ScoringCommandResult("Nutanix", ScoringCommandStatus.NEW,
                outputTable, 100, new Timestamp(System.currentTimeMillis()));
        this.scoringCommandResultEntityMgr.create(scoringCommandResult);
        this.scoringManager.cleanTables();
        assertEquals(this.scoringCommandResultEntityMgr.findAll().size(), 1);
        assertEquals(this.scoringCommandLogService.findByScoringCommand(scoringCommand).size(), 1);
        assertEquals(this.scoringCommandStateEntityMgr.findAll().size(), 3);

        this.metadataService.createNewEmptyTableFromExistingOne(this.scoringJdbcTemplate, outputTable,
                this.testOutputTable);
        assertEquals(this.metadataService.showTable(this.scoringJdbcTemplate, outputTable).get(0), outputTable);

        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommand.setConsumed(new Timestamp(System.currentTimeMillis()));
        scoringCommandResult.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandResult.setConsumed(new Timestamp(System.currentTimeMillis()));
        this.scoringCommandEntityMgr.update(scoringCommand);
        this.scoringCommandResultEntityMgr.update(scoringCommandResult);
        Thread.sleep(4000);
        this.scoringManager.cleanTables();

        assertEquals(this.scoringCommandStateEntityMgr.findAll().size(), 0);
        assertEquals(this.scoringCommandLogService.findByScoringCommand(scoringCommand).size(), 0);
        assertNull(this.scoringCommandEntityMgr.findByKey(scoringCommand));
        assertNull(this.scoringCommandResultEntityMgr.findByKey(scoringCommandResult));
        assertEquals(this.metadataService.showTable(this.scoringJdbcTemplate, inputTable).size(), 0);
        assertEquals(this.metadataService.showTable(this.scoringJdbcTemplate, outputTable).size(), 0);
    }

    @Test(groups = "functional", enabled = true)
    public void testHandleJobFailed() throws ParseException {
        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.POPULATED, inputTable, 0,
                100, new Timestamp(System.currentTimeMillis()));
        this.scoringCommandEntityMgr.create(scoringCommand);

        this.scoringCommandLogService.log(scoringCommand, "message.  #%#$%%^$%^$%^$%^");
        this.scoringCommandLogService.log(scoringCommand, "another message.  #%#$%%^$%^$%^$%^ 12344       .");

        this.scoringProcessor.setScoringCommand(scoringCommand);
        String failedAppId = "application_1415144508340_0729";
        ScoringCommandState scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.SCORE_DATA);
        this.scoringCommandStateEntityMgr.create(scoringCommandState);
        PagerDutyTestUtils.confirmPagerDutyIncident(this.scoringProcessor.handleJobFailed(failedAppId));
        PagerDutyTestUtils.confirmPagerDutyIncident(this.scoringProcessor.handleJobFailed());
    }

    @Test(groups = "functional", enabled = true)
    public void checkIfModelGuidExists() {
        assertFalse(metadataService.checkIfColumnExists(scoringJdbcTemplate, "LeadInputQueue", "Model_GUID"));
        assertTrue(metadataService.checkIfColumnExists(scoringJdbcTemplate, testInputTable,
                ScoringDaemonService.MODEL_GUID));
        assertEquals(
                metadataService.getDistinctColumnValues(scoringJdbcTemplate, testInputTable,
                        ScoringDaemonService.MODEL_GUID).get(0), "ms__1e8e6c34-80ec-4f5b-b979-e79c8cc6bec3-PLSModel");
    }

}
