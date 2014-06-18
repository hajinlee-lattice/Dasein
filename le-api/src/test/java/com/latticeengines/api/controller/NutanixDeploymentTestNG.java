package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingElement;
import com.latticeengines.domain.exposed.dataplatform.algorithm.RandomForestAlgorithm;

public class NutanixDeploymentTestNG extends ApiFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(CopyOfNutanixDeploymentTestNG.class);
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private MetadataService metadataService;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${api.rest.endpoint.hostport}")
    private String restEndpointHost;

    private Model model;

    protected boolean doYarnClusterSetup() {
        return false;
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(customerBaseDir + "/Nutanix"), true);

        ModelDefinition modelDef = produceModelDef();
        this.model = produceAModel(modelDef);
    }

    protected Model produceAModel(ModelDefinition modelDef) {
        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Nutanix Random Forest Model on Depivoted Data");
        model.setTable("Q_EventTable_Nutanix");
        model.setMetadataTable("EventMetadata");
        model.setCustomer("Nutanix");
        model.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        model.setDataFormat("avro");
        
        return model;
    }

    protected ModelDefinition produceModelDef() {
        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("all");
        randomForestAlgorithm.setAlgorithmProperties("criterion=gini n_estimators=10 n_jobs=4 min_samples_split=25 min_samples_leaf=10 bootstrap=True");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Logistic regression against all");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm }));
        return modelDef;
    }
    
    private Pair<String, List<String>> getTargetAndFeatures() {
        log.info("               info..............."+this.getClass().getSimpleName()+"getTargetAndFeatures");
        StringList features = restTemplate.postForObject("http://" + restEndpointHost + "/rest/features", model,
                StringList.class, new Object[] {});
        return new Pair<String, List<String>>("P1_Event", features.getElements());
    }
    
    @Test(groups = "deployment", enabled = true)
    public void load() throws Exception {
        log.info("               info..............."+this.getClass().getSimpleName()+"load");
        LoadConfiguration config = getLoadConfig();
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/load", config,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus state = platformTestBase.waitForStatus(appId, 360, TimeUnit.SECONDS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(state, FinalApplicationStatus.SUCCEEDED);
    }

    private LoadConfiguration getLoadConfig() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("10.41.1.250").port(1433).db("dataplatformtest").user("root").password("welcome");
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("Nutanix");
        config.setTable("Q_EventTable_Nutanix");
        config.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        return config;
    }

    @Test(groups = "deployment", dependsOnMethods = { "load" }, enabled = true)
    public void createSamples() throws Exception {
        log.info("               info..............."+this.getClass().getSimpleName()+"createSamples");
        
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        samplingConfig.addSamplingElement(all);
        samplingConfig.setCustomer(this.model.getCustomer());
        samplingConfig.setTable(this.model.getTable());
        
        AppSubmission submission = restTemplate.postForObject(
                "http://" + restEndpointHost + "/rest/createSamples", samplingConfig, AppSubmission.class,
                new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus state = platformTestBase.waitForStatus(appId, 240, TimeUnit.SECONDS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(state, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "createSamples" })
    public void profile() throws Exception {
        log.info("               info..............."+this.getClass().getSimpleName()+"profile");
        
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(this.model.getCustomer());
        config.setTable(this.model.getTable());
        config.setMetadataTable(this.model.getMetadataTable());
        config.setSamplePrefix("all");
        config.setExcludeColumnList(ModelingServiceTestUtils.createExcludeList());
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/profile", config,
                AppSubmission.class, new Object[] {});
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, 120, TimeUnit.SECONDS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }
    
    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "profile" })
    public void submit() throws Exception {
        log.info("               info..............."+this.getClass().getSimpleName()+"submit");
        
        Pair<String, List<String>> targetAndFeatures = getTargetAndFeatures();
        this.model.setFeaturesList(targetAndFeatures.getValue());
        this.model.setTargetsList(Arrays.<String> asList(new String[] { targetAndFeatures.getKey() }));
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/submit",
                this.model, AppSubmission.class, new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            FinalApplicationStatus state = platformTestBase.waitForStatus(appId, 240, TimeUnit.SECONDS,
                    FinalApplicationStatus.SUCCEEDED);
            assertEquals(state, FinalApplicationStatus.SUCCEEDED);
        }
    }

    class Pair<K, V> {
        private K key;
        private V value;
        
        public Pair(K key, V value) {
            this.setKey(key);
            this.setValue(value);
        }

        public K getKey() {
            return key;
        }

        public void setKey(K key) {
            this.key = key;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V value) {
            this.value = value;
        }
    }


}
