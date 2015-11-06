package com.latticeengines.scoring.orchestration.service.impl;

import static org.testng.Assert.assertTrue;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
import com.latticeengines.scoring.orchestration.service.ScoringValidationService;

public class ScoringValidationServiceImplTestNG extends ScoringFunctionalTestNGBase {

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private ScoringValidationService scoringValidationService;

    private String inputLeadsTable;

    private String inputLeadsTableWithOutModelGuid = "inputLeadsTableWithOutModelGuid";

    private String inputLeadsTableWithOutLeadId = "inputLeadsTableWithOutLeadId";

    private String emptyTableName = "ScoreOutput";

    private static final String customer = "Mulesoft_Relaunch";

    @SuppressWarnings("unused")
    private String tenant;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() throws Exception {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenant = CustomerSpace.parse(customer).toString();
        inputLeadsTable = getClass().getSimpleName() + "_LeadsTable";
        metadataService.createNewTableFromExistingOne(scoringJdbcTemplate, inputLeadsTable, testInputTable);
    }

    @Test(groups = "functional")
    public void testValidateTotal() throws Exception {
        int totalCommandNumber = Integer.parseInt(metadataService.getRowCount(scoringJdbcTemplate, inputLeadsTable)
                .toString());
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable,
                0, totalCommandNumber, new Timestamp(System.currentTimeMillis()));
        try {
            scoringValidationService.validateBeforeProcessing(scoringCommand);
            assertTrue(true, "Should not throw exception");
        } catch (Exception e) {
            Assert.fail("Should NOT have thrown exception");
        }

        scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTableWithOutModelGuid,
                0, totalCommandNumber, new Timestamp(System.currentTimeMillis()));
        try {
            scoringValidationService.validateBeforeProcessing(scoringCommand);
            Assert.fail("Should have thrown expcetion.");
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(((LedpException) e).getCode() == LedpCode.LEDP_20004);
        }

        scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTableWithOutLeadId, 0,
                totalCommandNumber, new Timestamp(System.currentTimeMillis()));
        try {
            scoringValidationService.validateBeforeProcessing(scoringCommand);
            Assert.fail("Should have thrown expcetion.");
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(((LedpException) e).getCode() == LedpCode.LEDP_20003);
        }

        scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, inputLeadsTable, 0,
                totalCommandNumber - 1, new Timestamp(System.currentTimeMillis()));
        try {
            scoringValidationService.validateBeforeProcessing(scoringCommand);
            Assert.fail("Should have thrown expcetion.");
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(((LedpException) e).getCode() == LedpCode.LEDP_20016);
        }

        scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, emptyTableName, 0, 0,
                new Timestamp(System.currentTimeMillis()));
        try {
            scoringValidationService.validateBeforeProcessing(scoringCommand);
            Assert.fail("Should have thrown expcetion.");
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(((LedpException) e).getCode() == LedpCode.LEDP_20017);
        }
    }

    @AfterClass(enabled = true, alwaysRun = true)
    public void cleanup() {
        try {
            metadataService.dropTable(scoringJdbcTemplate, inputLeadsTable);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
