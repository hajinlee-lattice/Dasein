package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.impl.AlertServiceImpl;
import com.latticeengines.dataplatform.exposed.service.impl.PagerDutyTestUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandLog;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import static org.testng.Assert.assertEquals;

public class ModelCommandCallableMethodTestNG extends DataPlatformFunctionalTestNGBase {

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
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Autowired
    private DebugProcessorImpl debugProcessorImpl;

    @Autowired
    private AlertServiceImpl alertService;

    @Value("${dataplatform.fs.web.defaultFS}")
    private String httpFsPrefix;

    @Value("${dataplatform.yarn.resourcemanager.webapp.address}")
    private String resourceManagerWebAppAddress;

    @Value("${dataplatform.yarn.timeline-service.webapp.address}")
    private String appTimeLineWebAppAddress;

    private static final String TEMP_EVENTTABLE = "ModelCommandCallableTestNG_eventtable";

    @BeforeClass(groups = "functional")
    public void setup() throws SQLException {
        alertService.enableTestMode();
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

    @Test(groups = "functional")
    public void testPopulateDataSize() throws SQLException {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(TEMP_EVENTTABLE);
        ModelCommandCallable callable = new ModelCommandCallable(command, yarnConfiguration, modelingJobService,
                modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor, modelCommandLogService,
                modelCommandResultEntityMgr, modelStepFinishProcessor, modelStepOutputResultsProcessor,
                modelStepRetrieveMetadataProcessor, debugProcessorImpl, alertService, httpFsPrefix,
                resourceManagerWebAppAddress, appTimeLineWebAppAddress);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());
        callable.populateDataSize(commandParameters);
        List<ModelCommandLog> logs = modelCommandLogService.findByModelCommand(command);
        String log = logs.get(logs.size() - 1).getMessage();
        if (log.contains("KB")) {
            // Data Size: 16384 KB Row count: 160 Column count: 6 (SQLServer)
            long dataSize = Long.valueOf(log.split(":")[1].trim().split("KB")[0].trim());
            if (dataSize > 1000) {
                assertEquals(command.getDataSize(), Math.round(dataSize / 1000.0));
            } else {
                assertEquals(command.getDataSize(), 1L);
            }
        } else {
            // Data Size: 16384 Row count: 160 Column count: 6 (MySQL)
            long dataSize = Long.valueOf(log.split(":")[1].trim().split(" ")[0].trim());
            if (dataSize > 1000000L) {
                assertEquals(command.getDataSize(), Math.round(dataSize / 1000000.0));
            } else {
                assertEquals(command.getDataSize(), 1L);
            }
        }
    }

    @Test(groups = "functional")
    public void testHandleJobFailed() throws ParseException {
        ModelCommand command = new ModelCommand(1L, "Nutanix", ModelCommandStatus.NEW, null, ModelCommand.TAHOE);
        command.setModelCommandStep(ModelCommandStep.PROFILE_DATA);
        modelCommandEntityMgr.create(command);

        modelCommandLogService.log(command, "message.  #%#$%%^$%^$%^$%^");
        modelCommandLogService.log(command, "another message.  #%#$%%^$%^$%^$%^ 12344       .");

        ModelCommandResult result = new ModelCommandResult(command, new Date(), new Date(),
                ModelCommandStatus.IN_PROGRESS);
        modelCommandResultEntityMgr.create(result);

        ModelCommandCallable callable = new ModelCommandCallable(command, yarnConfiguration, modelingJobService,
                modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor, modelCommandLogService,
                modelCommandResultEntityMgr, modelStepFinishProcessor, modelStepOutputResultsProcessor,
                modelStepRetrieveMetadataProcessor, debugProcessorImpl, alertService, httpFsPrefix,
                resourceManagerWebAppAddress, appTimeLineWebAppAddress);

        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed());

        List<String> failedAppIds = new ArrayList<String>();
        failedAppIds.add("application_1415144508340_0729");
        PagerDutyTestUtils.confirmPagerDutyIncident(callable.handleJobFailed(failedAppIds));
    }
}
