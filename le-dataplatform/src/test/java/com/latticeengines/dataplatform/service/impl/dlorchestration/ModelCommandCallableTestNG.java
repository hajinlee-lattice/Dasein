package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.StandaloneHttpServer;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataServlet;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;

public class ModelCommandCallableTestNG extends DataPlatformFunctionalTestNGBase {

    private static final String TEMP_EVENTTABLE = "ModelCommandCallableTestNG_eventtable";

    @Autowired
    private JobService jobService;

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

        String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
        if (dbDriverName.contains("Microsoft")) {
            // Microsoft JDBC Driver 4.0 for SQL Server
            dlOrchestrationJdbcTemplate.execute("select * into " + TEMP_EVENTTABLE + " from Q_EventTable_Nutanix");
        } else {
            // MySQL Connector Java
            dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE + " select * from Q_EventTable_Nutanix");
        }

    }

    @AfterClass(groups = "functional")
    public void cleanup() throws Exception {
        super.cleanup();
        httpServer.stop();
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE);
    }

    @Test(groups = "functional")
    public void testWorkflow() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(TEMP_EVENTTABLE);
        modelCommandEntityMgr.create(command);
        // Comment out above 2 lines and uncomment next block when testing against an integration database
        /*
        List<ModelCommand> commands = modelCommandEntityMgr.getNewAndInProgress();
        ModelCommand command = commands.get(0);
        */
        ModelCommandCallable modelCommandCallable = new ModelCommandCallable(command, jobService,
                modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor, modelCommandLogService,
                modelCommandResultEntityMgr, modelStepFinishProcessor, modelStepOutputResultsProcessor,
                modelStepRetrieveMetadataProcessor);

        int iterations = 0;
        while ((command.getCommandStatus() == ModelCommandStatus.NEW || command.getCommandStatus() == ModelCommandStatus.IN_PROGRESS)
                && iterations < 400) {
            modelCommandCallable.call();
            iterations++;
            Thread.sleep(15000);
        }

        assertTrue(command.getCommandStatus() == ModelCommandStatus.SUCCESS);

        List<ModelCommandLog> logs = modelCommandLogEntityMgr.findAll();
        assertEquals(logs.size(), 14);
        List<ModelCommandState> states = modelCommandStateEntityMgr.findAll();
        assertEquals(states.size(), 7);
    }

}
