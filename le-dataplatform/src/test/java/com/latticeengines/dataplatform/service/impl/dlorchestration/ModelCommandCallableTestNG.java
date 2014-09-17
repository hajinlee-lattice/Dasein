package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.StandaloneHttpServer;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataServlet;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

@ContextConfiguration(locations = { "classpath:dataplatform-dlorchestration-quartz-context.xml" })
public class ModelCommandCallableTestNG extends DataPlatformFunctionalTestNGBase {

    private static final String TEMP_EVENTTABLE = "ModelCommandCallableTestNG_eventtable";

    @Autowired
    private ModelingJobService modelingJobService;

    @Autowired
    private ModelStepYarnProcessor modelStepYarnProcessor;

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    @Autowired
    private ModelStepProcessor modelStepFinishProcessor;

    @Autowired
    private ModelStepProcessor modelStepOutputResultsProcessor;

    @Autowired
    private ModelStepProcessor modelStepRetrieveMetadataProcessor;

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
        } else {
            // MySQL Connector Java
            dlOrchestrationJdbcTemplate.execute("drop table if exists " + TEMP_EVENTTABLE);
            dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE
                    + " select * from Q_EventTable_Nutanix");
        }

    }

    @AfterClass(groups = "functional")
    public void cleanup() throws Exception {
        httpServer.stop();
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE);
    }

    @Override
    @AfterMethod(enabled = true, alwaysRun = true)
    public void afterEachTest() {
        super.clearTables();
    }

    @Test(groups = "functional")
    public void testWorkflow() throws Exception {
        // Comment out below 2 lines when testing against an integration
        // database
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(TEMP_EVENTTABLE);
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
                System.out.println(modelCommandLog.getMessage());
            }
        }
        assertTrue(command.getCommandStatus() == ModelCommandStatus.SUCCESS,
                "The actual command state is " + command.getCommandStatus());

        List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
        assertEquals(logs.size(), 14);
        List<ModelCommandState> states = modelCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 7);
        List<ModelCommandResult> results = modelCommandResultEntityMgr.findAll();
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getProcessStatus(), ModelCommandStatus.SUCCESS);
    }

    @Test(groups = "functional")
    public void testMissingParameters() throws Exception {
        ModelCommand command = new ModelCommand(1L, "Nutanix", ModelCommandStatus.NEW, null, ModelCommand.TAHOE);
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
