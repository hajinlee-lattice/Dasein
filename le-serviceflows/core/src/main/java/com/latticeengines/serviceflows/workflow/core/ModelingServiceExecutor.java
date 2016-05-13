package com.latticeengines.serviceflows.workflow.core;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.serviceflows.workflow.modeling.ProductType;
import com.latticeengines.serviceflows.workflow.modeling.ProvenanceProperties;

public class ModelingServiceExecutor {

    // TODO externalize this as a step configuration property
    private static final int MAX_SECONDS_WAIT_FOR_MODELING = 60 * 60 * 24;

    private static final Log log = LogFactory.getLog(ModelingServiceExecutor.class);

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
        fs.delete(
                new Path(String.format("%s/%s/data/%s", modelingServiceHdfsBaseDir, builder.getCustomer(),
                        builder.getTable())), true);
    }

    public void runPipeline() throws Exception {
        writeMetadataFiles();
        loadData();
        sample();
        profile();
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

    @SuppressWarnings("deprecation")
    public void loadData() throws Exception {
        LoadConfiguration config = new LoadConfiguration();

        DbCreds.Builder credsBldr = new DbCreds.Builder();
        credsBldr.host(builder.getHost()) //
                .port(builder.getPort()) //
                .db(builder.getDb()) //
                .user(builder.getUser()) //
                .password(builder.getPassword()) //
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
        AppSubmission submission = modelProxy.profile(config);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for profile: %s", appId));
        waitForAppId(appId);
        return appId;
    }

    public String model() throws Exception {
        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);

        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Random Forest against all");
        modelDef.addAlgorithms(Collections.singletonList((Algorithm) randomForestAlgorithm));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName(builder.getModelName());
        if (builder.getDisplayName() != null) {
            model.setDisplayName(builder.getDisplayName());
        }
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        model.setKeyCols(Arrays.asList(new String[] { builder.getKeyColumn() }));
        model.setDataFormat("avro");
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
        String provenanceProperties = StringUtils.join(props, " ");
        provenanceProperties += " " + ProvenanceProperties.valueOf(builder.getProductType()).getResolvedProperties();

        model.setProvenanceProperties(provenanceProperties);

        AbstractMap.SimpleEntry<List<String>, List<String>> targetAndFeatures = getTargetAndFeatures();
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
            
            if (modeling) {
                status = modelProxy.getJobStatus(appId);
            } else {
                status = jobProxy.getJobStatus(appId);
            }
            
            Thread.sleep(1000L);
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

    private AbstractMap.SimpleEntry<List<String>, List<String>> getTargetAndFeatures() {
        Model model = new Model();
        model.setName("Model-" + System.currentTimeMillis());
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        StringList features = modelProxy.getFeatures(model);
        return new AbstractMap.SimpleEntry<>(Arrays.asList(builder.getTargets()), features.getElements());
    }

    public static class Builder {

        private String host;
        private int port;
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
        private String productType;

        private String loadSubmissionUrl = "/rest/load";
        private String modelSubmissionUrl = "/rest/submit";
        private String sampleSubmissionUrl = "/rest/createSamples";
        private String profileSubmissionUrl = "/rest/profile";
        private String retrieveFeaturesUrl = "/rest/features";
        private String retrieveJobStatusUrl = "/rest/getJobStatus/%s";
        private String modelingJobStatusUrl = "/rest/getJobStatus/%s";
        private String hdfsDirToSample;
        
        private ModelProxy modelProxy;
        private JobProxy jobProxy;

        public Builder() {
        }

        public Builder dataSourceHost(String host) {
            this.setHost(host);
            return this;
        }

        public Builder dataSourcePort(int port) {
            this.setPort(port);
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

        public Builder productType(String productType) {
            this.setProductType(productType);
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

        public void setSourceSchemaInterpretation(String sourceSchemaInterpretation) {
            this.sourceSchemaInterpretation = sourceSchemaInterpretation;
        }

        public String getSourceSchemaInterpretation() {
            return sourceSchemaInterpretation;
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

    }
}
