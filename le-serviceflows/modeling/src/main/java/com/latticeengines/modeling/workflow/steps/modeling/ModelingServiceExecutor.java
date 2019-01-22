package com.latticeengines.modeling.workflow.steps.modeling;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.domain.exposed.mapreduce.counters.WorkflowCounter;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.EventCounterConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.ModelReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.SamplingProperty;
import com.latticeengines.domain.exposed.modeling.SamplingType;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.domain.exposed.modeling.factory.AlgorithmFactory;
import com.latticeengines.domain.exposed.modeling.factory.PipelineFactory;
import com.latticeengines.domain.exposed.modeling.factory.SamplingFactory;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenance;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;

public class ModelingServiceExecutor {

    private static final Logger log = LoggerFactory.getLogger(ModelingServiceExecutor.class);

    // TODO externalize this as a step configuration property
    private static final int MAX_SECONDS_WAIT_FOR_MODELING = 60 * 60 * 24;

    private static final String SAMPLING_COUNTER_GROUP_NAME = "ledp.sampling.counter";

    protected Builder builder;

    protected String modelingServiceHostPort;

    protected String modelingServiceHdfsBaseDir;

    protected JobProxy jobProxy;

    protected ModelProxy modelProxy;

    protected Configuration yarnConfiguration;

    public ModelingServiceExecutor(Builder builder) {
        this.builder = builder;
        this.modelingServiceHostPort = builder.getModelingServiceHostPort();
        this.modelingServiceHdfsBaseDir = builder.getModelingServiceHdfsBaseDir();
        this.yarnConfiguration = builder.getYarnConfiguration();
        this.jobProxy = builder.getJobProxy();
        this.modelProxy = builder.getModelProxy();
    }

    public void init() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(
                String.format("%s/%s/data/%s", modelingServiceHdfsBaseDir, builder.getCustomer(), builder.getTable())),
                true);
    }

    public void cleanCustomerDataDir() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(
                String.format("%s/%s/data/%s", modelingServiceHdfsBaseDir, builder.getCustomer(), builder.getTable())),
                true);
    }

    public void runPipeline() throws Exception {
        writeMetadataFiles();
        loadData();
        sample();
        profile();
        review();
        model();
    }

    public void writeMetadataFiles() throws Exception {
        String metadataHdfsPath = String.format("%s/%s/data/%s/metadata.avsc", modelingServiceHdfsBaseDir,
                builder.getCustomer(), builder.getMetadataTable());
        String rtsHdfsPath = String.format("%s/%s/data/%s/datacomposition.json", modelingServiceHdfsBaseDir,
                builder.getCustomer(), builder.getMetadataTable());
        HdfsUtils.writeToFile(yarnConfiguration, metadataHdfsPath, builder.getMetadataContents());
        HdfsUtils.writeToFile(yarnConfiguration, rtsHdfsPath, builder.getDataCompositionContents());
    }

    public void loadData() throws Exception {
        LoadConfiguration config = new LoadConfiguration();

        DbCreds.Builder credsBldr = new DbCreds.Builder();
        credsBldr.host(builder.getHost()) //
                .port(builder.getPort()) //
                .db(builder.getDb()) //
                .user(builder.getUser()) //
                .clearTextPassword(builder.getPassword()) //
                .dbType(builder.getDbType());
        DbCreds creds = new DbCreds(credsBldr);
        config.setCreds(creds);
        config.setCustomer(builder.getCustomer());
        config.setTable(builder.getTable());
        config.setMetadataTable(builder.getMetadataTable());
        config.setKeyCols(Collections.singletonList(builder.getKeyColumn()));
        AppSubmission submission = modelProxy.loadData(config);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for load: %s", appId));
        waitForAppId(appId);
    }

    public String eventCounter() throws Exception {
        EventCounterConfiguration eventCounterConfig = new EventCounterConfiguration();
        eventCounterConfig.setCustomer(builder.getCustomer());
        eventCounterConfig.setTable(builder.getTable());
        eventCounterConfig.setHdfsDirPath(builder.getHdfsDirToSample());
        eventCounterConfig.setParallelEnabled(true);
        eventCounterConfig.setProperty(SamplingProperty.TARGET_COLUMN_NAME.name(), builder.getEventColumn());
        eventCounterConfig.setProperty(SamplingProperty.COUNTER_GROUP_NAME.name(),
                WorkflowCounter.SAMPLING_COUNTER_GROUP_NAME.name());

        AppSubmission submission = modelProxy.createEventCounter(eventCounterConfig);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for eventCounter: %s", appId));
        waitForAppId(appId);

        if (builder.getCounterGroupResultMap() != null) {
            Counters counters = jobProxy.getMRJobCounters(appId.toString());
            CounterGroup counterGroup = counters.getAllCounterGroups()
                    .get(WorkflowCounter.SAMPLING_COUNTER_GROUP_NAME.name());
            Map<String, Counter> counterMap = counterGroup.getAllCounters();
            final Map<String, Long> counterGroupResultMap = builder.getCounterGroupResultMap();

            counterMap.keySet().stream().forEach(key -> {
                Counter c = counterMap.get(key);
                counterGroupResultMap.put(c.getName(), c.getValue());
            });

            log.info(String.format("eventCounter result: %s", JsonUtils.serialize(counterGroupResultMap)));
        }
        return appId;
    }

    public String sample() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        samplingConfig.addSamplingElement(all);
        samplingConfig.setCustomer(builder.getCustomer());
        samplingConfig.setTable(builder.getTable());
        samplingConfig.setHdfsDirPath(builder.getHdfsDirToSample());
        samplingConfig.setParallelEnabled(true);
        samplingConfig.setProperty(SamplingProperty.COUNTER_GROUP_NAME.name(), SAMPLING_COUNTER_GROUP_NAME);
        samplingConfig.setProperty(SamplingProperty.TARGET_COLUMN_NAME.name(), builder.getEventColumn());
        if (builder.getSamplingType() != null) {
            samplingConfig.setSamplingType(builder.getSamplingType());
        }
        samplingConfig.setCounterGroupResultMap(builder.getCounterGroupResultMap());
        SamplingFactory.configSampling(samplingConfig, builder.runTimeParams);
        log.info(String.format("Configuration for sampling: %s", JsonUtils.serialize(samplingConfig)));
        AppSubmission submission = modelProxy.createSamples(samplingConfig);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for sampling: %s", appId));
        waitForAppId(appId);
        return appId;
    }

    public String profile() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(builder.getCustomer());
        config.setTable(builder.getTable());
        config.setMetadataTable(builder.getMetadataTable());
        config.setExcludeColumnList(Arrays.asList(builder.getProfileExcludeList()));
        config.setSamplePrefix("all");
        config.setTargets(Arrays.asList(builder.getTargets()));
        config.setParallelEnabled(true);
        if (builder.isV2ProfilingEnabled()) {
            config.setScript("/datascience/dataplatform/scripts/algorithm/data_profile_v2.py");
        }
        AppSubmission submission = modelProxy.profile(config);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for profile: %s", appId));
        waitForAppId(appId);
        return appId;
    }

    public String review() throws Exception {
        ModelReviewConfiguration config = new ModelReviewConfiguration();
        config.setCustomer(builder.getCustomer());
        config.setTable(builder.getTable());
        config.setMetadataTable(builder.getMetadataTable());
        config.setSamplePrefix("all");
        config.setTargets(Arrays.asList(builder.getTargets()));
        config.setDataRules(builder.getDataRules());
        AppSubmission submission = modelProxy.review(config);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for review: %s", appId));
        waitForAppId(appId);
        return appId;
    }

    @VisibleForTesting
    String getEnabledRulesAsPipelineProp(List<DataRule> dataRules) {
        String enabledRulesProp = "";
        if (CollectionUtils.isNotEmpty(dataRules)) {
            Map<String, List<String>> enabledMandatoryRules = new HashMap<>();
            List<String> customerPredictors = new ArrayList<>();
            for (DataRule dataRule : dataRules) {
                if (dataRule.isEnabled() && dataRule.hasMandatoryRemoval()) {
                    enabledMandatoryRules.put(dataRule.getName(), dataRule.getFlaggedColumnNames() == null
                            ? new ArrayList<String>() : dataRule.getFlaggedColumnNames());
                    for (String customerPredictor : dataRule.getCustomerPredictors()) {
                        if (!customerPredictors.contains(customerPredictor))
                            customerPredictors.add(customerPredictor);
                    }
                }
            }
            enabledRulesProp = String.format("remediatedatarulesstep.enabledRules=%s",
                    JsonUtils.serialize(enabledMandatoryRules));
            enabledRulesProp += String.format(" remediatedatarulesstep.customerPredictors=%s",
                    JsonUtils.serialize(customerPredictors));
        }
        return enabledRulesProp;
    }

    void setPipelineProperties(Algorithm algorithm) {
        // RemediateDataRuleStep has been removed from the pipeline for now.
        if (builder.dataCloudVersion != null && builder.dataCloudVersion.startsWith("2")) {
            if (!algorithm.getPipelineProperties().isEmpty()) {
                algorithm.setPipelineProperties(algorithm.getPipelineProperties() + " ");
            }
            String[] pipelineProperties = new String[] { //
                    String.format("featureselectionstep.enabled=%s", true), //
                    String.format("assignconversionratetoallcategoricalvalues.enabled=%s", true), //
                    String.format("enumeratedcolumntransformstep.enabled=%s", false), //
                    String.format("categoricalgroupingstep.enabled=%s", !builder.isSkipStandardTransform()), //
                    String.format("pivotstep.enabled=%s", !builder.isSkipStandardTransform()), //
                    String.format("unmatchedselectionstep.enabled=%s", true) //
            };

            log.info(String.format("Model: %s, Pipeline properties: %s", builder.modelName,
                    JsonUtils.serialize(pipelineProperties)));
            algorithm.setPipelineProperties(StringUtils.join(pipelineProperties, " "));
        }
    }

    public String model() throws Exception {
        if (builder.isCrossSellModel) {
            return crossSellModel();
        }

        Algorithm algorithm = getAlgorithm();
        setPipelineProperties(algorithm);

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Random Forest against all");
        modelDef.addAlgorithms(Collections.singletonList(algorithm));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName(builder.getModelName());
        if (builder.getDisplayName() != null) {
            model.setDisplayName(builder.getDisplayName());
        }
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        model.setKeyCols(Arrays.asList(builder.getKeyColumn()));
        model.setDataFormat("avro");

        setProvenanceProperties(model, null);
        return submitModel(model);
    }

    public String crossSellModel() throws Exception {
        Algorithm algorithm = getCdlAlgorithm();
        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Random Forest against all");
        modelDef.addAlgorithms(Collections.singletonList(algorithm));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName(builder.getModelName());
        if (builder.getDisplayName() != null) {
            model.setDisplayName(builder.getDisplayName());
        }
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        model.setKeyCols(Arrays.asList(builder.getKeyColumn()));
        model.setDataFormat("avro");
        model.setParallelEnabled(true);
        model.setFeaturesThreshold(50);
        String extraProperties = "DataLoader_Query=x DataLoader_TenantName=" + builder.getCustomer()
                + " DataLoader_Instance=z";
        extraProperties += " Expected_Value=" + builder.isExpectedValue();
        setProvenanceProperties(model, extraProperties);
        return submitModel(model);
    }

    private Algorithm getCdlAlgorithm() {
        Algorithm algorithm = new RandomForestAlgorithm();
        algorithm.setSampleName("all");
        algorithm.setScript("/datascience/dataplatform/scripts/algorithm/parallel_rf_train.py");
        if (builder.isExpectedValue()) {
            algorithm.setPipelineScript("/datascience/playmaker/evmodel/evpipeline.py");
            algorithm.setPipelineLibScript("/datascience/playmaker/evmodel/evpipeline.tar.gz");
            log.info("This is EV model!.");
        }
        algorithm.setPriority(2);
        List<String> properties = new ArrayList<>();
        properties.add("criterion=gini");
        properties.add("n_estimators=100");
        properties.add("n_jobs=5");
        properties.add("min_samples_split=25");
        properties.add("min_samples_leaf=10");
        properties.add("max_depth=8");
        properties.add("bootstrap=True");
        properties.add("calibration_width=4");
        algorithm.setAlgorithmProperties(String.join(" ", properties));
        return algorithm;
    }

    private String submitModel(Model model) throws Exception, InterruptedException {
        AbstractMap.SimpleEntry<List<String>, List<String>> targetAndFeatures = getTargetAndFeatures(
                model.getFeaturesThreshold());
        model.setTargetsList(targetAndFeatures.getKey());
        model.setFeaturesList(targetAndFeatures.getValue());

        AppSubmission submission = modelProxy.submit(model);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for modeling: %s", appId));
        JobStatus status = waitForModelingAppId(appId);
        // Wait for 30 seconds before retrieving the result directory
        Thread.sleep(30 * 1000L);
        String resultDir = status.getResultDirectory();

        if (resultDir != null) {
            return appId;
        } else {
            log.warn(String.format("No result directory for modeling job %s", appId));
            System.out.println(String.format("No result directory for modeling job %s", appId));
            throw new LedpException(LedpCode.LEDP_28014, new String[] { appId });
        }
    }

    private void setProvenanceProperties(Model model, String extraProperties) {
        List<String> props = new ArrayList<>();
        if (builder.getEventTableTable() != null) {
            props.add("Event_Table_Name=" + builder.getEventTableTable());
        }
        if (builder.getSourceSchemaInterpretation() != null) {
            props.add("Source_Schema_Interpretation=" + builder.getSourceSchemaInterpretation());
        }
        if (builder.getTrainingTableName() != null) {
            props.add("Training_Table_Name=" + builder.getTrainingTableName());
        }
        if (builder.getTargetTableName() != null) {
            props.add("Target_Table_Name=" + builder.getTargetTableName());
        }
        if (builder.getPredefinedColumnSelection() != null) {
            props.add("Predefined_ColumnSelection_Name=" + builder.getPredefinedColumnSelection().getName());
            props.add("Predefined_ColumnSelection_Version=" + builder.getPredefinedSelectionVersion());
        } else if (builder.getCustomizedColumnSelection() != null) {
            props.add("Customized_ColumnSelection=" + JsonUtils.serialize(builder.getCustomizedColumnSelection()));
        }
        if (builder.getPivotArtifactPath() != null) {
            props.add(ProvenancePropertyName.PivotFilePath.getName() + "=" + builder.getPivotArtifactPath());
        }
        if (builder.getModuleName() != null) {
            props.add("Module_Name=" + builder.getModuleName());
        }
        if (builder.dataCloudVersion != null) {
            props.add("Data_Cloud_Version=" + builder.dataCloudVersion);
        }

        String provenanceProperties = StringUtils.join(props, " ");
        provenanceProperties += " " + ProvenanceProperties.valueOf(builder.getProductType()).getResolvedProperties();
        provenanceProperties += builder.modelSummaryProvenance.getProvenancePropertyString();
        if (builder.getJobId() != null) {
            provenanceProperties += " Workflow_Job_Id=" + Long.toString(builder.getJobId());
        }
        if (extraProperties != null)
            provenanceProperties += " " + extraProperties;
        log.info("The model provenance property is: " + provenanceProperties);

        model.setProvenanceProperties(provenanceProperties);
    }

    private Algorithm getAlgorithm() {
        Algorithm algorithm = AlgorithmFactory.createAlgorithm(builder.runTimeParams);
        PipelineFactory.configPipeline(algorithm, builder.runTimeParams);
        return algorithm;
    }

    protected JobStatus waitForAppId(String appId) throws Exception {
        return waitForAppId(appId, false);
    }

    protected JobStatus waitForModelingAppId(String appId) throws Exception {
        return waitForAppId(appId, true);
    }

    protected JobStatus waitForAppId(String appId, boolean modeling) throws Exception {
        JobStatus status;
        int maxTries = MAX_SECONDS_WAIT_FOR_MODELING;
        int i = 0;
        do {
            Thread.sleep(30_000L);
            if (modeling) {
                status = modelProxy.getJobStatus(appId);
            } else {
                status = jobProxy.getJobStatus(appId);
            }
            i++;
            if (i == maxTries) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));

        if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
            throw new LedpException(LedpCode.LEDP_28010, new String[] { appId, status.getStatus().toString() });
        }

        return status;
    }

    private AbstractMap.SimpleEntry<List<String>, List<String>> getTargetAndFeatures(int featuresThreshold) {
        Model model = new Model();
        model.setName("Model-" + System.currentTimeMillis());
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        model.setFeaturesThreshold(featuresThreshold);
        StringList features = modelProxy.getFeatures(model);
        return new AbstractMap.SimpleEntry<>(Arrays.asList(builder.getTargets()), features.getElements());
    }

    public static class Builder {

        private String host;
        private int port;
        private Long jobId;
        private String db;
        private String user;
        private String password;
        private String dbType;
        private String customer;
        private String table;
        private String metadataTable;
        private String keyColumn;
        private String[] profileExcludeList;
        private String[] targets;
        private String[] featureList;
        private String modelingServiceHostPort;
        private String modelingServiceHdfsBaseDir;
        private Configuration yarnConfiguration;
        private String metadataContents;
        private String dataCompositionContents;
        private String schemaContents;
        private String modelName;
        private String displayName;
        private String eventTableName;
        private String sourceSchemaInterpretation;
        private String trainingTableName;
        private String targetTableName;
        private String productType;
        private String transformationGroupName;
        private boolean skipStandardTransform;
        private boolean v2ProfilingEnabled;
        private boolean isCrossSellModel;
        private boolean expectedValue;
        private Predefined predefinedColumnSelection;
        private String predefinedSelectionVersion;
        private ColumnSelection customizedColumnSelection;
        private ModelSummaryProvenance modelSummaryProvenance;

        private String loadSubmissionUrl = "/rest/load";
        private String modelSubmissionUrl = "/rest/submit";
        private String sampleSubmissionUrl = "/rest/createSamples";
        private String profileSubmissionUrl = "/rest/profile";
        private String retrieveFeaturesUrl = "/rest/features";
        private String retrieveJobStatusUrl = "/rest/getJobStatus/%s";
        private String modelingJobStatusUrl = "/rest/getJobStatus/%s";
        private String hdfsDirToSample;
        private String pivotArtifactPath;

        private ModelProxy modelProxy;
        private JobProxy jobProxy;
        private Map<ArtifactType, String> metadataArtifacts;
        private List<DataRule> dataRules;
        private Map<String, String> runTimeParams;
        private String dataCloudVersion;
        private String moduleName;
        private String eventColumn;
        private Map<String, Long> counterGroupResultMap;
        private SamplingType samplingType;

        public Builder() {
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            this.setV2ProfilingEnabled(v2ProfilingEnabled);
            return this;
        }

        public Builder crossSellModel(boolean isCrossSellModel) {
            this.setCrossSellModel(isCrossSellModel);
            return this;
        }

        public Builder expectedValue(boolean expectedValue) {
            this.setExpectedValue(expectedValue);
            return this;
        }

        public Builder transformationGroupName(String transformationGroupName) {
            this.setTransformationGroupName(transformationGroupName);
            return this;
        }

        public Builder skipStandardTransform(boolean skipStandardTransform) {
            this.setSkipStandardTransform(skipStandardTransform);
            return this;
        }

        public Builder dataSourceHost(String host) {
            this.setHost(host);
            return this;
        }

        public Builder dataSourcePort(int port) {
            this.setPort(port);
            return this;
        }

        public Builder jobId(Long jobId) {
            this.setJobId(jobId);
            return this;
        }

        public Builder dataSourceDb(String db) {
            this.setDb(db);
            return this;
        }

        public Builder dataSourceUser(String user) {
            this.setUser(user);
            return this;
        }

        public Builder dataSourcePassword(String password) {
            this.setPassword(password);
            return this;
        }

        public Builder dataSourceDbType(String dbType) {
            this.setDbType(dbType);
            return this;
        }

        public Builder customer(String customer) {
            this.setCustomer(customer);
            return this;
        }

        public Builder table(String table) {
            this.setTable(table);
            return this;
        }

        public Builder metadataTable(String metadataTable) {
            this.setMetadataTable(metadataTable);
            return this;
        }

        public Builder keyColumn(String keyColumn) {
            this.setKeyColumn(keyColumn);
            return this;
        }

        public Builder profileExcludeList(String... profileExcludeList) {
            this.setProfileExcludeList(profileExcludeList);
            return this;
        }

        public Builder featureList(String... featureList) {
            this.setFeatureList(featureList);
            return this;
        }

        public Builder modelingServiceHostPort(String modelingServiceHostPort) {
            this.setModelingServiceHostPort(modelingServiceHostPort);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            this.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder targets(String... targets) {
            this.setTargets(targets);
            return this;
        }

        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.setYarnConfiguration(yarnConfiguration);
            return this;
        }

        public Builder metadataContents(String metadataContents) {
            this.setMetadataContents(metadataContents);
            return this;
        }

        public Builder dataCompositionContents(String dataCompositionContents) {
            this.setDataCompositionContents(dataCompositionContents);
            return this;
        }

        public Builder avroSchema(String avroSchema) {
            this.setSchemaContents(avroSchema);
            return this;
        }

        public Builder modelName(String modelName) {
            this.setModelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            this.setDisplayName(displayName);
            return this;
        }

        public Builder modelSubmissionUrl(String modelSubmissionUrl) {
            this.setModelSubmissionUrl(modelSubmissionUrl);
            return this;
        }

        public Builder sampleSubmissionUrl(String sampleSubmissionUrl) {
            this.setSampleSubmissionUrl(sampleSubmissionUrl);
            return this;
        }

        public Builder profileSubmissionUrl(String profileSubmissionUrl) {
            this.setProfileSubmissionUrl(profileSubmissionUrl);
            return this;
        }

        public Builder loadSubmissionUrl(String loadSubmissionUrl) {
            this.setLoadSubmissionUrl(loadSubmissionUrl);
            return this;
        }

        public Builder retrieveFeaturesUrl(String retrieveFeaturesUrl) {
            this.setRetrieveFeaturesUrl(retrieveFeaturesUrl);
            return this;
        }

        public Builder retrieveJobStatusUrl(String retrieveJobStatusUrl) {
            this.setRetrieveJobStatusUrl(retrieveJobStatusUrl);
            return this;
        }

        public Builder hdfsDirToSample(String hdfsDirToSample) {
            this.setHdfsDirToSample(hdfsDirToSample);
            return this;
        }

        public Builder retrieveModelingJobStatusUrl(String modelingJobStatusUrl) {
            this.setModelingJobStatusUrl(modelingJobStatusUrl);
            return this;
        }

        public Builder eventTableName(String eventTableName) {
            this.setEventTableName(eventTableName);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            this.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            this.setTrainingTableName(trainingTableName);
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            this.setTargetTableName(targetTableName);
            return this;
        }

        public Builder productType(String productType) {
            this.setProductType(productType);
            return this;
        }

        public Builder eventColumn(String eventColumn) {
            this.setEventColumn(eventColumn);
            return this;
        }

        public Builder modelProxy(ModelProxy modelProxy) {
            this.setModelProxy(modelProxy);
            return this;
        }

        public Builder jobProxy(JobProxy jobProxy) {
            this.setJobProxy(jobProxy);
            return this;
        }

        public Builder predefinedColumnSelection(Predefined predefined, String version) {
            this.setPredefinedColumnSelection(predefined);
            this.setPredefinedSelectionVersion(version);
            return this;
        }

        public Builder metadataArtifacts(Map<ArtifactType, String> metadataArtifacts) {
            this.setMetadataArtifacts(metadataArtifacts);
            return this;
        }

        public Builder customizedColumnSelection(ColumnSelection columnSelection) {
            if (this.getPredefinedColumnSelection() != null) {
                log.warn("When both predefined and customized column selections are provided, "
                        + "customized one will be ignored");
            }
            this.setCustomizedColumnSelection(columnSelection);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            this.setPivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder moduleName(String moduleName) {
            this.setModuleName(moduleName);
            return this;
        }

        public Builder counterGroupResultMap(Map<String, Long> counterGroupResultMap) {
            this.setCounterGroupResultMap(counterGroupResultMap);
            return this;
        }

        public void setPivotArtifactPath(String pivotArtifactPath) {
            this.pivotArtifactPath = pivotArtifactPath;
        }

        public String getPivotArtifactPath() {
            return pivotArtifactPath;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            this.dataRules = dataRules;
            return this;
        }

        public Builder runTimeParams(Map<String, String> runTimeParams) {
            this.runTimeParams = runTimeParams;
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            this.dataCloudVersion = dataCloudVersion;
            return this;
        }

        public Builder setModelSummaryProvenance(ModelSummaryProvenance modelSummaryProvenance) {
            this.modelSummaryProvenance = modelSummaryProvenance;
            return this;
        }

        public Builder samplingType(SamplingType samplingType) {
            this.setSamplingType(samplingType);
            return this;
        }

        public void setHdfsDirToSample(String hdfsDirToSample) {
            this.hdfsDirToSample = hdfsDirToSample;
        }

        public String getHdfsDirToSample() {
            return hdfsDirToSample;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public void setJobId(Long jobId) {
            this.jobId = jobId;
        }

        public Long getJobId() {
            return jobId;
        }

        public String getDb() {
            return db;
        }

        public void setDb(String db) {
            this.db = db;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDbType() {
            return dbType;
        }

        public void setDbType(String dbType) {
            this.dbType = dbType;
        }

        public String getCustomer() {
            return customer;
        }

        public void setCustomer(String customer) {
            this.customer = customer;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getMetadataTable() {
            return metadataTable;
        }

        public void setMetadataTable(String metadataTable) {
            this.metadataTable = metadataTable;
        }

        public String getKeyColumn() {
            return keyColumn;
        }

        public void setKeyColumn(String keyColumn) {
            this.keyColumn = keyColumn;
        }

        public String[] getProfileExcludeList() {
            return profileExcludeList;
        }

        public void setProfileExcludeList(String... profileExcludeList) {
            this.profileExcludeList = profileExcludeList;
        }

        public String[] getFeatureList() {
            return featureList;
        }

        public void setFeatureList(String... featureList) {
            this.featureList = featureList;
        }

        public String[] getTargets() {
            return targets;
        }

        public void setTargets(String[] targets) {
            this.targets = targets;
        }

        public String getModelingServiceHostPort() {
            return modelingServiceHostPort;
        }

        public void setModelingServiceHostPort(String modelingServiceHostPort) {
            this.modelingServiceHostPort = modelingServiceHostPort;
        }

        public String getModelingServiceHdfsBaseDir() {
            return modelingServiceHdfsBaseDir;
        }

        public void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
        }

        public Configuration getYarnConfiguration() {
            return yarnConfiguration;
        }

        public void setYarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
        }

        public String getMetadataContents() {
            return metadataContents;
        }

        public void setMetadataContents(String metadataContents) {
            this.metadataContents = metadataContents;
        }

        public String getDataCompositionContents() {
            return dataCompositionContents;
        }

        public void setDataCompositionContents(String dataCompositionContents) {
            this.dataCompositionContents = dataCompositionContents;
        }

        public String getSchemaContents() {
            return schemaContents;
        }

        public void setSchemaContents(String schemaContents) {
            this.schemaContents = schemaContents;
        }

        public String getModelName() {
            return modelName;
        }

        public void setModelName(String modelName) {
            this.modelName = modelName;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public String getModelSubmissionUrl() {
            return modelSubmissionUrl;
        }

        public void setModelSubmissionUrl(String modelSubmissionUrl) {
            this.modelSubmissionUrl = modelSubmissionUrl;
        }

        public String getSampleSubmissionUrl() {
            return sampleSubmissionUrl;
        }

        public void setSampleSubmissionUrl(String sampleSubmissionUrl) {
            this.sampleSubmissionUrl = sampleSubmissionUrl;
        }

        public String getProfileSubmissionUrl() {
            return profileSubmissionUrl;
        }

        public void setProfileSubmissionUrl(String profileSubmissionUrl) {
            this.profileSubmissionUrl = profileSubmissionUrl;
        }

        public String getRetrieveFeaturesUrl() {
            return retrieveFeaturesUrl;
        }

        public void setRetrieveFeaturesUrl(String retrieveFeaturesUrl) {
            this.retrieveFeaturesUrl = retrieveFeaturesUrl;
        }

        public String getRetrieveJobStatusUrl() {
            return retrieveJobStatusUrl;
        }

        public void setRetrieveJobStatusUrl(String retrieveJobStatusUrl) {
            this.retrieveJobStatusUrl = retrieveJobStatusUrl;
        }

        public String getLoadSubmissionUrl() {
            return loadSubmissionUrl;
        }

        public void setLoadSubmissionUrl(String loadSubmissionUrl) {
            this.loadSubmissionUrl = loadSubmissionUrl;
        }

        public void setModelingJobStatusUrl(String modelingJobStatusUrl) {
            this.modelingJobStatusUrl = modelingJobStatusUrl;
        }

        public String getModelingJobStatusUrl() {
            return modelingJobStatusUrl;
        }

        public void setEventTableName(String eventTableName) {
            this.eventTableName = eventTableName;
        }

        public String getEventTableTable() {
            return eventTableName;
        }

        public void setTrainingTableName(String trainingTableName) {
            this.trainingTableName = trainingTableName;
        }

        public String getTrainingTableName() {
            return trainingTableName;
        }

        public String getTargetTableName() {
            return targetTableName;
        }

        public void setTargetTableName(String targetTableName) {
            this.targetTableName = targetTableName;
        }

        public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
            this.sourceSchemaInterpretation = sourceSchemaInterpretation;
        }

        public String getSourceSchemaInterpretation() {
            return sourceSchemaInterpretation;
        }

        public String getTransformationGroupName() {
            return transformationGroupName;
        }

        public void setTransformationGroupName(String transformationGroupName) {
            this.transformationGroupName = transformationGroupName;
        }

        public boolean isSkipStandardTransform() {
            return skipStandardTransform;
        }

        public void setSkipStandardTransform(boolean skipStandardTransform) {
            this.skipStandardTransform = skipStandardTransform;
        }

        public boolean isV2ProfilingEnabled() {
            return v2ProfilingEnabled;
        }

        public void setV2ProfilingEnabled(boolean v2ProfilingEnabled) {
            this.v2ProfilingEnabled = v2ProfilingEnabled;
        }

        public boolean isCrossSellModel() {
            return isCrossSellModel;
        }

        public void setCrossSellModel(boolean isCrossSellModel) {
            this.isCrossSellModel = isCrossSellModel;
        }

        public boolean isExpectedValue() {
            return expectedValue;
        }

        public void setExpectedValue(boolean expectedValue) {
            this.expectedValue = expectedValue;
        }

        public void setProductType(String productType) {
            this.productType = productType;
        }

        public String getProductType() {
            if (productType == null) {
                productType = ProductType.LP.name();
            }
            return productType;
        }

        public String getEventColumn() {
            return eventColumn;
        }

        public void setEventColumn(String eventColumn) {
            this.eventColumn = eventColumn;
        }

        public ColumnSelection getCustomizedColumnSelection() {
            return customizedColumnSelection;
        }

        public void setCustomizedColumnSelection(ColumnSelection customizedColumnSelection) {
            this.customizedColumnSelection = customizedColumnSelection;
        }

        public Predefined getPredefinedColumnSelection() {
            return predefinedColumnSelection;
        }

        public void setPredefinedColumnSelection(Predefined predefinedColumnSelection) {
            this.predefinedColumnSelection = predefinedColumnSelection;
        }

        public String getPredefinedSelectionVersion() {
            return predefinedSelectionVersion;
        }

        public void setPredefinedSelectionVersion(String predefinedSelectionVersion) {
            this.predefinedSelectionVersion = predefinedSelectionVersion;
        }

        public void setModelProxy(ModelProxy modelProxy) {
            this.modelProxy = modelProxy;
        }

        public ModelProxy getModelProxy() {
            return modelProxy;
        }

        public void setJobProxy(JobProxy jobProxy) {
            this.jobProxy = jobProxy;
        }

        public JobProxy getJobProxy() {
            return jobProxy;
        }

        public void setMetadataArtifacts(Map<ArtifactType, String> metadataArtifacts) {
            this.metadataArtifacts = metadataArtifacts;
        }

        public Map<ArtifactType, String> getMetadataArtifacts() {
            return metadataArtifacts;
        }

        public List<DataRule> getDataRules() {
            return dataRules;
        }

        public String getModuleName() {
            return moduleName;
        }

        public void setModuleName(String moduleName) {
            this.moduleName = moduleName;
        }

        public Map<String, Long> getCounterGroupResultMap() {
            return counterGroupResultMap;
        }

        public void setCounterGroupResultMap(Map<String, Long> counterGroupResultMap) {
            this.counterGroupResultMap = counterGroupResultMap;
        }

        public SamplingType getSamplingType() {
            return samplingType;
        }

        public void setSamplingType(SamplingType samplingType) {
            this.samplingType = samplingType;
        }
    }
}
