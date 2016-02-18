package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceTestUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataServlet;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.testframework.rest.StandaloneHttpServer;

@ContextConfiguration(locations = { "classpath:dataplatform-dlorchestration-quartz-context.xml" })
public class ModelCommandCallableTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ModelCommandCallableTestNG.class);

    private static final String TEMP_EVENTTABLE = "ModelCommandCallableTestNG_eventtable";

    private static final String TEMP_EVENTTABLE_FEW_ROWS = "ModelCommandCallableTestNG_eventtable_fewrows";

    @Autowired
    private ModelCommandLogEntityMgr modelCommandLogEntityMgr;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    private StandaloneHttpServer httpServer;

    private RestTemplate restTemplate = new RestTemplate();

    protected boolean doDependencyLibraryCopy() {
        return false;
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        httpServer = new StandaloneHttpServer();
        httpServer.init();
        String[] cols = new String[] { "A", "B", "C" };
        Integer[] types = new Integer[] { 0, 0, 0 };
        httpServer.addServlet(new VisiDBMetadataServlet(cols, types), "/DLRestService/GetQueryMetaDataColumns");
        httpServer.start();

        super.clearTables();
        String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
        if (dbDriverName.contains("Microsoft")) {
            // Microsoft JDBC Driver 4.0 for SQL Server
            dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + TEMP_EVENTTABLE + "', 'U') IS NOT NULL DROP TABLE "
                    + TEMP_EVENTTABLE);
            dlOrchestrationJdbcTemplate.execute("select * into " + TEMP_EVENTTABLE + " from Q_EventTable_Nutanix");
            dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + TEMP_EVENTTABLE_FEW_ROWS
                    + "', 'U') IS NOT NULL DROP TABLE " + TEMP_EVENTTABLE_FEW_ROWS);
            dlOrchestrationJdbcTemplate.execute("select * into " + TEMP_EVENTTABLE_FEW_ROWS
                    + " from Q_EventTableDepivot_Nutanix_FewRows");
        } else {
            // MySQL Connector Java
            dlOrchestrationJdbcTemplate.execute("drop table if exists " + TEMP_EVENTTABLE);
            dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE
                    + " select * from Q_EventTable_Nutanix");
            dlOrchestrationJdbcTemplate.execute("drop table if exists " + TEMP_EVENTTABLE_FEW_ROWS);
            dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE_FEW_ROWS
                    + " select * from Q_EventTableDepivot_Nutanix_FewRows");
        }

    }

    @AfterClass(groups = "functional")
    public void cleanup() throws Exception {
        httpServer.stop();
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE);
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE_FEW_ROWS);
    }

    @Override
    @AfterMethod(enabled = true, alwaysRun = true)
    public void afterEachTest() {
        super.clearTables();
    }

    @Test(groups = "functional", enabled = true)
    public void testWorkflowValidationFailed() throws Exception {
        // Comment out below 2 lines when testing against an integration
        // database
        // Validation failure due to too few rows
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithFewRowsAndReadoutTargets(1L,
                TEMP_EVENTTABLE_FEW_ROWS, false, true);
        modelCommandEntityMgr.create(command);

        int iterations = 0;
        while ((command.getCommandStatus() == ModelCommandStatus.NEW || command.getCommandStatus() == ModelCommandStatus.IN_PROGRESS)
                && iterations < 100) { // Wait maximum of 25 minutes to process
                                       // this command
            iterations++;
            Thread.sleep(15000);
            command = modelCommandEntityMgr.findByKey(command);
        }

        if (command.getCommandStatus() == ModelCommandStatus.FAIL) {
            List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
            for (ModelCommandLog modelCommandLog : logs) {
                log.info(modelCommandLog.getMessage());
            }
        }
        assertTrue(command.getCommandStatus() == ModelCommandStatus.FAIL,
                "The actual command state is " + command.getCommandStatus());

        List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
        assertEquals(logs.size(), 2);
        List<ModelCommandState> states = modelCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 0);
        List<ModelCommandResult> results = modelCommandResultEntityMgr.findAll();
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getProcessStatus(), ModelCommandStatus.FAIL);
    }

    @Test(groups = "functional", dependsOnMethods = { "testWorkflowValidationFailed" })
    public void testWorkflow() throws Exception {
        // Note that this test changes the event table that is shared with first
        // test case and has to be run after
        // Comment out below 2 lines when testing against an integration
        // database
        // Set test flag to disable validation
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L, TEMP_EVENTTABLE,
                false, false);
        modelCommandEntityMgr.create(command);

        int iterations = 0;
        while ((command.getCommandStatus() == ModelCommandStatus.NEW || command.getCommandStatus() == ModelCommandStatus.IN_PROGRESS)
                && iterations < 100) { // Wait maximum of 25 minutes to process
                                       // this command
            iterations++;
            Thread.sleep(15000);
            command = modelCommandEntityMgr.findByKey(command);
        }

        if (command.getCommandStatus() == ModelCommandStatus.FAIL) {
            List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
            for (ModelCommandLog modelCommandLog : logs) {
                log.info(modelCommandLog.getMessage());
            }
        }
        assertTrue(command.getCommandStatus() == ModelCommandStatus.SUCCESS,
                "The actual command state is " + command.getCommandStatus());

        List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
        assertTrue(logs.size() >= 15);
        for (ModelCommandLog log : logs) {
            String message = log.getMessage();
            if (message.contains("http")) {
                String link = message.substring(message.indexOf("http"));
                ResponseEntity<String> response = restTemplate.getForEntity(link, String.class);
                assertTrue(response.getStatusCode().equals(HttpStatus.OK));
            }
        }
        List<ModelCommandState> states = modelCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 7);
        List<ModelCommandResult> results = modelCommandResultEntityMgr.findAll();
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getProcessStatus(), ModelCommandStatus.SUCCESS);
    }

    @Test(groups = "functional")
    public void testMissingParameters() throws Exception {
        ModelCommand command = new ModelCommand(1L, "Nutanix", "Nutanix", ModelCommandStatus.NEW, null,
                ModelCommand.TAHOE, ModelingServiceTestUtils.EVENT_TABLE);
        modelCommandEntityMgr.create(command);

        int iterations = 0;
        while ((command.getCommandStatus() == ModelCommandStatus.NEW || command.getCommandStatus() == ModelCommandStatus.IN_PROGRESS)
                && iterations < 20) { // Wait maximum of 5 minutes to process
                                      // this command
            iterations++;
            Thread.sleep(15000);
            command = modelCommandEntityMgr.findByKey(command);
        }

        assertTrue(command.getCommandStatus() == ModelCommandStatus.FAIL,
                "The actual command state is " + command.getCommandStatus());

        List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
        assertTrue(logs.get(0).getMessage().contains("LEDP_16000"));
        assertTrue(logs.get(1).getMessage().contains("FAIL"));
        assertEquals(logs.size(), 2);
        List<ModelCommandState> states = modelCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 0);
        List<ModelCommandResult> results = modelCommandResultEntityMgr.findAll();
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getProcessStatus(), ModelCommandStatus.FAIL);
    }

}
