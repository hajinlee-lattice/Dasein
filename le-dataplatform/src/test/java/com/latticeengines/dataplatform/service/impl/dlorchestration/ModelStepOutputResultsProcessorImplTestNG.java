package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelStepOutputResultsProcessorImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final String YARN_APPLICATION_ID = "yarnApplicationId";
    private static final String TEMP_EVENTTABLE = "ModelStepOutputResultsProcessorImplTestNG_eventtable";

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private ModelStepProcessor modelStepOutputResultsProcessor;

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Mock
    private ModelingService modelingService;

    private RestTemplate restTemplate = new RestTemplate();

    private List<String> contents = Arrays.<String> asList(new String[] { "a", "b", "c", "d" });

    @BeforeClass(groups = "functional")
    public void beforeClass() throws Exception {
        initMocks(this);
        JobStatus jobStatus = new JobStatus();
        jobStatus.setResultDirectory("/test/dl");
        HdfsUtils.writeToFile(yarnConfiguration, "/test/dl/testmodel.json", contents.get(0));
        HdfsUtils.writeToFile(yarnConfiguration, "/test/dl/testmodel.csv", contents.get(1));
        HdfsUtils.writeToFile(yarnConfiguration, "/test/dl/scored.txt", contents.get(2));
        HdfsUtils.writeToFile(yarnConfiguration, "/test/dl/readoutsample.csv", contents.get(3));
        when(modelingService.getJobStatus(YARN_APPLICATION_ID)).thenReturn(jobStatus);

        ReflectionTestUtils.setField(modelStepOutputResultsProcessor, "modelingService", modelingService);

        String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
        if (dbDriverName.contains("Microsoft")) {
            // Microsoft JDBC Driver 4.0 for SQL Server
            dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + TEMP_EVENTTABLE + "', 'U') IS NOT NULL DROP TABLE "
                    + TEMP_EVENTTABLE);
        } else {
            // MySQL Connector Java
            dlOrchestrationJdbcTemplate.execute("drop table if exists " + TEMP_EVENTTABLE);
        }
        dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE + " (Id int)");
    }

    @AfterClass(groups = { "functional" })
    public void cleanup() throws Exception {
        dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE);
        HdfsUtils.rmdir(yarnConfiguration, "/test/dl");
        super.clearTables();
    }

    @Test(groups = "functional")
    public void testExecutePostStep() throws Exception {
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(TEMP_EVENTTABLE);
        modelCommandEntityMgr.create(command);
        ModelCommandParameters commandParameters = new ModelCommandParameters(command.getCommandParameters());

        ModelCommandState commandState = new ModelCommandState(command, ModelCommandStep.SUBMIT_MODELS);
        commandState.setYarnApplicationId(YARN_APPLICATION_ID);
        modelCommandStateEntityMgr.createOrUpdate(commandState);

        modelStepOutputResultsProcessor.executeStep(command, commandParameters);

        String outputAlgorithm = dlOrchestrationJdbcTemplate.queryForObject("select Algorithm from "
                + commandParameters.getEventTable(), String.class);
        assertEquals(outputAlgorithm, ModelStepOutputResultsProcessorImpl.RANDOM_FOREST);

        List<String> outputLogs = dlOrchestrationJdbcTemplate.queryForList("select Message from LeadScoringCommandLog",
                String.class);
        assertEquals(outputLogs.size(), 4);
        for (int i = 0; i < outputLogs.size(); i++) {
            String link = outputLogs.get(i).substring(outputLogs.get(i).indexOf("http://"));
            assertEquals(restTemplate.getForObject(link, String.class), contents.get(i));
        }
    }
}
