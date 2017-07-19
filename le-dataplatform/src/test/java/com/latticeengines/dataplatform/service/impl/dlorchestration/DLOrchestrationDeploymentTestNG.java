package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandIdEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataServlet;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandId;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.testframework.exposed.rest.StandaloneHttpServer;

@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DLOrchestrationDeploymentTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Autowired
    private ModelCommandIdEntityMgr modelCommandIdEntityMgr;

    private StandaloneHttpServer httpServer;

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    private static final String TEMP_EVENTTABLE = "DLOrchestrationDeploymentTestNG_eventtable";

    private static final Logger log = LoggerFactory.getLogger(DLOrchestrationDeploymentTestNG.class);

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        httpServer = new StandaloneHttpServer();
        httpServer.init();
        String[] cols = new String[] { "A", "B", "C" };
        Integer[] types = new Integer[] { 0, 0, 0 };
        httpServer.addServlet(new VisiDBMetadataServlet(cols, types), "/DLRestService/GetQueryMetaDataColumns");
        httpServer.start();

        String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
        if (dbDriverName.contains("Microsoft")) {
            // Microsoft JDBC Driver 4.0 for SQL Server
            dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + TEMP_EVENTTABLE + "', 'U') IS NOT NULL DROP TABLE "
                    + TEMP_EVENTTABLE);
            dlOrchestrationJdbcTemplate.execute("select * into " + TEMP_EVENTTABLE + " from Q_EventTable_Nutanix");
        } else {
            // MySQL Connector Java
            dlOrchestrationJdbcTemplate.execute("drop table if exists " + TEMP_EVENTTABLE);
            dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE
                    + " select * from Q_EventTable_Nutanix");
        }
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws Exception {
        httpServer.stop();
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE);
    }

    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        // Note that this test changes the event table that is shared with first
        // test case and has to be run after
        // Comment out below 2 lines when testing against an integration
        // database
        // Set test flag to disable validation
        ModelCommandId commandId = ModelingServiceTestUtils.createModelCommandId();
        modelCommandIdEntityMgr.create(commandId);
        log.info(String.valueOf(commandId.getPid()));
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParametersFeatureSelection(
                commandId.getPid(), TEMP_EVENTTABLE, false, false);
        modelCommandEntityMgr.create(command);
        log.info("data populated, testing started.");
        int iterations = 0;
        while ((command.getCommandStatus() == ModelCommandStatus.NEW || command.getCommandStatus() == ModelCommandStatus.IN_PROGRESS)
                && iterations < 100) { // Wait maximum of 25 minutes to process
                                       // this command
            iterations++;
            Thread.sleep(15000);
            command = modelCommandEntityMgr.findByKey(command);
        }

        if (command.getCommandStatus() == ModelCommandStatus.FAIL) {
            List<ModelCommandLog> logs = modelCommandLogService.findByModelCommand(command);
            for (ModelCommandLog modelCommandLog : logs) {
                log.info(modelCommandLog.getMessage());
            }
        }
        assertTrue(command.getCommandStatus() == ModelCommandStatus.SUCCESS,
                "The actual command state is " + command.getCommandStatus());

        List<ModelCommandLog> logs = modelCommandLogService.findByModelCommand(command);
        assertTrue(logs.size() >= 15);
        for (ModelCommandLog log : logs) {
            String message = log.getMessage();
            if (message.contains("http")) {
                String link = message.substring(message.indexOf("http"));
                System.out.println(link);
                ResponseEntity<String> response = restTemplate.getForEntity(link, String.class);
                assertTrue(response.getStatusCode().equals(HttpStatus.OK));
            }
        }
        List<ModelCommandStep> steps = Arrays.<ModelCommandStep> asList(ModelCommandStep.RETRIEVE_METADATA,
                ModelCommandStep.LOAD_DATA, ModelCommandStep.GENERATE_SAMPLES, ModelCommandStep.PROFILE_DATA,
                ModelCommandStep.SUBMIT_MODELS, ModelCommandStep.OUTPUT_COMMAND_RESULTS, ModelCommandStep.FINISH);
        for (ModelCommandStep step : steps) {
            assertTrue(
                    modelCommandStateEntityMgr.findByModelCommandAndStep(command, step).get(0).getStatus()
                            .equals(FinalApplicationStatus.SUCCEEDED), String.format("%s is not successful", step));
        }
        ModelCommandResult result = modelCommandResultEntityMgr.findByModelCommand(command);
        assertNotNull(result);
        assertEquals(result.getProcessStatus(), ModelCommandStatus.SUCCESS);
    }
}
