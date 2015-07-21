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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.interfaces.data.DataInterfaceSubscriber;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceTestUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
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

    private String pmmlContents = "XML!";
    private String modelSummaryContents = "MSC";
    private String scoreDerivationContents = "Deciles!";
    private String dataCompositionContents = "Transforms!";
    private List<String> linkContents = Arrays.<String> asList(new String[] { "a", "b", "c", "d", "e", modelSummaryContents });

    private String resultDirectory = "/user/s-analytics/customers/Nutanix.Nutanix.Production/models/Q_EventTable_Nutanix/58e6de15-5448-4009-a512-bd27d59abcde/";
    private String consumerDirectory = "/user/s-analytics/customers/Nutanix/BARD/58e6de15-5448-4009-a512-bd27d59abcde-Model_Su/";   
    private String hdfsArtifactsDirectory = "/user/s-analytics/customers/Nutanix.Nutanix.Production/models/58e6de15-5448-4009-a512-bd27d59abcde-Model_Su/1/";
    private String zkArtifactsPath = "/Models/58e6de15-5448-4009-a512-bd27d59abcde-Model_Su/1/";

    @BeforeClass(groups = "functional")
    public void beforeClass() throws Exception {
        initMocks(this);
        JobStatus jobStatus = new JobStatus();
        jobStatus.setResultDirectory(resultDirectory);

        HdfsUtils.rmdir(yarnConfiguration, consumerDirectory);
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "testmodel.json", linkContents.get(0));
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "testmodel.csv", linkContents.get(1));
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "diagnostics.json", linkContents.get(2));
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "testscored.txt", linkContents.get(3));
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "testreadoutsample.csv", linkContents.get(4));
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "rfpmml.xml", pmmlContents);
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "enhancements/modelsummary.json",
                modelSummaryContents);
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "enhancements/ScoreDerivation.json",
                scoreDerivationContents);
        HdfsUtils.writeToFile(yarnConfiguration, resultDirectory + "enhancements/DataComposition.json",
                dataCompositionContents);
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
        HdfsUtils.rmdir(yarnConfiguration, resultDirectory);
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

        String outputAlgorithm = dlOrchestrationJdbcTemplate.queryForObject(
                "select Algorithm from " + command.getEventTable(), String.class);
        assertEquals(outputAlgorithm, ModelStepOutputResultsProcessorImpl.RANDOM_FOREST);

        List<String> outputLogs = dlOrchestrationJdbcTemplate.queryForList("select Message from LeadScoringCommandLog",
                String.class);
        assertEquals(outputLogs.size(), 6);
        for (int i = 0; i < outputLogs.size(); i++) {
            String link = outputLogs.get(i).substring(outputLogs.get(i).indexOf("http://"));
            assertEquals(restTemplate.getForObject(link, String.class), linkContents.get(i));
        }

        checkModel(command);
        checkHdfsArtifacts(command);
        checkZkArtifacts(command);
    }

    private void checkModel(ModelCommand command) throws Exception {

        String modelFile = consumerDirectory + "1.json";
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
        Assert.assertEquals(content, linkContents.get(0));
    }

    private void checkHdfsArtifacts(ModelCommand command) throws Exception {
        String pmmlFile = hdfsArtifactsDirectory + "ModelPmml.xml";
        String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, pmmlFile);
        Assert.assertEquals(content, pmmlContents);
    }

    private void checkZkArtifacts(ModelCommand command) throws Exception {
        String interfaceName = "ModelArtifact";
        CustomerSpace space = CustomerSpace.parse(command.getDeploymentExternalId());
        DataInterfaceSubscriber subscriber = new DataInterfaceSubscriber(interfaceName, space);

        Path relativePath = new Path(zkArtifactsPath + "ScoreDerivation.json");
        Document document = subscriber.get(relativePath);
        Assert.assertEquals(document.getData(), scoreDerivationContents);

        relativePath = new Path(zkArtifactsPath + "DataComposition.json");
        document = subscriber.get(relativePath);
        Assert.assertEquals(document.getData(), dataCompositionContents);
    }
}
