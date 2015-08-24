package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.StandaloneHttpServer;
import com.latticeengines.dataplatform.functionalframework.VisiDBMetadataServlet;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelCommandParameters;
import com.latticeengines.dataplatform.service.impl.dlorchestration.ModelStepRetrieveMetadataProcessorImpl;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

/**
 * This is an end-to-end test against a SQL Server database without having to go
 * through the REST API. It allows for an easier development-test cycle without
 * having to either deploy to Jetty or run from le-api.
 * 
 * @author rgonzalez
 * 
 */
@Transactional
public class ModelingServiceImplUnpivotedEndToEndTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelingJobService modelingJobService;

    @Autowired
    private YarnService yarnService;

    @Autowired
    private ModelingService modelingService;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private ModelEntityMgr modelEntityMgr;

    @Autowired
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Autowired
    private ModelStepRetrieveMetadataProcessorImpl modelStepRetrieveMetadataProcessor;

    @Autowired
    private MetadataService metadataService;

    private Model model = null;

    private StandaloneHttpServer httpServer;
    
    public String getCustomer() {
        return "Nutanix";
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path(String.format("%s/%s", customerBaseDir, getCustomer())), true);

        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        randomForestAlgorithm.setSampleName("s0");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model1");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm }));

        model = createModel(modelDef);
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        httpServer.stop();
    }

    private Model createModel(ModelDefinition modelDef) {
        Model m = new Model();
        m.setModelDefinition(modelDef);
        m.setName("Model Submission1");
        m.setTable("Q_EventTable_Nutanix");
        m.setMetadataTable("EventMetadata");
        m.setTargetsList(Arrays.<String> asList(new String[] { "P1_Event" }));
        m.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        m.setCustomer(getCustomer());
        m.setDataFormat("avro");

        return m;
    }

    private Pair<String[], Integer[]> getTableColumnMetadata() {
        DataSchema schema = metadataService.createDataSchema(getCreds(), "Q_EventTable_Nutanix");
        List<Field> fields = schema.getFields();
        String[] cols = new String[fields.size()];
        Integer[] types = new Integer[fields.size()];
        int i = 0;
        for (Field field : fields) {
            cols[i] = field.getName();
            types[i++] = field.getSqlType();
        }
        return new Pair<>(cols, types);
    }

    private DbCreds getCreds() {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost).port(dataSourcePort).db(dataSourceDB).user(dataSourceUser)
                .password(dataSourcePasswd).dbType(dataSourceDBType);
        return new DbCreds(builder);
    }

    private LoadConfiguration getLoadConfig() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds creds = getCreds();
        config.setCreds(creds);
        config.setCustomer(getCustomer());
        config.setTable("Q_EventTable_Nutanix");
        config.setMetadataTable("EventMetadata");
        config.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        return config;
    }

    @Test(groups = "functional", enabled = false, expectedExceptions = LedpException.class)
    public void loadBadTableInput() throws Exception {
        LoadConfiguration loadConfig = getLoadConfig();
        loadConfig.setTable("SomeBogusTableName");
        modelingService.loadData(loadConfig);
    }

    @Test(groups = "functional")
    public void retrieveMetadataAndWriteToHdfs() throws Exception {
        httpServer = new StandaloneHttpServer();
        httpServer.init();
        Pair<String[], Integer[]> colMetadata = getTableColumnMetadata();
        httpServer.addServlet(new VisiDBMetadataServlet(colMetadata.getFirst(), colMetadata.getSecond()),
                "/DLRestService/GetQueryMetaDataColumns");
        httpServer.start();
        ModelCommand command = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        modelCommandEntityMgr.createOrUpdate(command);
        List<ModelCommandParameter> commandParameters = command.getCommandParameters();
        modelStepRetrieveMetadataProcessor.executeStep(command, new ModelCommandParameters(commandParameters));
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "retrieveMetadataAndWriteToHdfs" })
    public void load() throws Exception {
        LoadConfiguration loadConfig = getLoadConfig();
        ApplicationId appId = modelingService.loadData(loadConfig);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "load" })
    public void createSamples() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        SamplingElement s2 = new SamplingElement();
        s2.setName("all");
        s2.setPercentage(100);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        samplingConfig.addSamplingElement(s2);
        samplingConfig.setCustomer(model.getCustomer());
        samplingConfig.setTable(model.getTable());
        ApplicationId appId = modelingService.createSamples(samplingConfig);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "createSamples" })
    public void profileData() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setExcludeColumnList(ModelingServiceTestUtils.createExcludeList());
        config.setSamplePrefix("all");
        config.setTargets(model.getTargetsList());
        ApplicationId appId = modelingService.profileData(config);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "profileData" })
    public void submitModel() throws Exception {
        List<String> features = modelingService.getFeatures(model, false);
        model.setFeaturesList(features);

        List<ApplicationId> appIds = modelingService.submitModel(model);

        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);

            JobStatus jobStatus = modelingService.getJobStatus(appId.toString());
            String modelFile = HdfsUtils.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory()).get(0);
            String modelContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }

}
