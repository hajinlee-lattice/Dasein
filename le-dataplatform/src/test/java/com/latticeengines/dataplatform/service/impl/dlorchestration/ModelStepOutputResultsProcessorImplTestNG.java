package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

    protected boolean doYarnClusterSetup() {
        return false;
    }

  @BeforeClass(groups = "functional")
  public void beforeClass() throws Exception {
      initMocks(this);
      JobStatus jobStatus = new JobStatus();
      jobStatus.setResultDirectory("/");
      when(modelingService.getJobStatus(YARN_APPLICATION_ID)).thenReturn(jobStatus);

      ReflectionTestUtils.setField(modelStepOutputResultsProcessor, "modelingService", modelingService);

      String dbDriverName = dlOrchestrationJdbcTemplate.getDataSource().getConnection().getMetaData().getDriverName();
      if (dbDriverName.contains("Microsoft")) {
          // Microsoft JDBC Driver 4.0 for SQL Server
          dlOrchestrationJdbcTemplate.execute("IF OBJECT_ID('" + TEMP_EVENTTABLE + "', 'U') IS NOT NULL DROP TABLE " + TEMP_EVENTTABLE);
      } else {
          // MySQL Connector Java
          dlOrchestrationJdbcTemplate.execute("drop table if exists " + TEMP_EVENTTABLE);
      }
      dlOrchestrationJdbcTemplate.execute("create table " + TEMP_EVENTTABLE + " (Id int)");
  }

  @AfterClass(groups = { "functional" })
  public void cleanup() {
      dlOrchestrationJdbcTemplate.execute("drop table " + TEMP_EVENTTABLE);
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

      String outputAlgorithm = dlOrchestrationJdbcTemplate.queryForObject("select Algorithm from " + commandParameters.getEventTable(), String.class);
      assertEquals(outputAlgorithm, ModelStepOutputResultsProcessorImpl.RANDOM_FOREST);
   }
 }
