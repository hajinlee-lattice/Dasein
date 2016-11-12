package com.latticeengines.pls.functionalframework;

import static org.testng.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.common.exposed.util.SSLUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.security.exposed.serviceruntime.exception.GetResponseErrorHandler;

public class ModelingServiceExecutor {

    private static final Log log = LogFactory.getLog(ModelingServiceExecutor.class);

    private Builder builder;

    private String modelingServiceHostPort;

    private String modelingServiceHdfsBaseDir;

    private RestTemplate restTemplate = SSLUtils.newSSLBlindRestTemplate();

    private Configuration yarnConfiguration;

    public ModelingServiceExecutor(Builder builder) {
        this.builder = builder;
        this.modelingServiceHostPort = builder.getModelingServiceHostPort();
        this.modelingServiceHdfsBaseDir = builder.getModelingServiceHdfsBaseDir();
        this.yarnConfiguration = builder.getYarnConfiguration();
        this.restTemplate.setErrorHandler(new GetResponseErrorHandler());
    }

    public void init() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(
                new Path(String.format("%s/%s/data/%s", modelingServiceHdfsBaseDir, builder.getCustomer(),
                        builder.getTable())), true);
    }

    public void runPipeline() throws Exception {
        writeMetadataFile();
        loadData();
        sample();
        profile();
        model();
    }

    public void writeMetadataFile() throws Exception {
        String hdfsPath = String.format("%s/%s/data/%s/metadata.avsc", modelingServiceHdfsBaseDir,
                builder.getCustomer(), builder.getMetadataTable());
        HdfsUtils.writeToFile(yarnConfiguration, hdfsPath, builder.getMetadataContents());
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
        AppSubmission submission = restTemplate.postForObject(modelingServiceHostPort + builder.getLoadSubmissionUrl(),
                config, AppSubmission.class);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for load: %s", appId));
        System.out.println(String.format("App id for load: %s", appId));
        waitForAppId(appId);
    }

    public void sample() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        samplingConfig.addSamplingElement(all);
        samplingConfig.setCustomer(builder.getCustomer());
        samplingConfig.setTable(builder.getTable());
        samplingConfig.setHdfsDirPath(builder.getHdfsDirToSample());
        AppSubmission submission = restTemplate.postForObject(
                modelingServiceHostPort + builder.getSampleSubmissionUrl(), samplingConfig, AppSubmission.class);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for sampling: %s", appId));
        System.out.println(String.format("App id for sampling: %s", appId));
        waitForAppId(appId);
    }

    public void profile() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(builder.getCustomer());
        config.setTable(builder.getTable());
        config.setMetadataTable(builder.getMetadataTable());
        config.setExcludeColumnList(Arrays.asList(builder.getProfileExcludeList()));
        config.setSamplePrefix("all");
        config.setTargets(Arrays.asList(builder.getTargets()));
        AppSubmission submission = restTemplate.postForObject(
                modelingServiceHostPort + builder.getProfileSubmissionUrl(), config, AppSubmission.class);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for profile: %s", appId));
        System.out.println(String.format("App id for profile: %s", appId));
        waitForAppId(appId);
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
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        model.setKeyCols(Arrays.asList(new String[] { builder.getKeyColumn() }));
        model.setDataFormat("avro");

        AbstractMap.SimpleEntry<List<String>, List<String>> targetAndFeatures = getTargetAndFeatures();
        model.setTargetsList(targetAndFeatures.getKey());
        model.setFeaturesList(targetAndFeatures.getValue());

        AppSubmission submission = restTemplate.postForObject(
                modelingServiceHostPort + builder.getModelSubmissionUrl(), model, AppSubmission.class);
        String appId = submission.getApplicationIds().get(0);
        log.info(String.format("App id for modeling: %s", appId));
        System.out.println(String.format("App id for modeling: %s", appId));
        JobStatus status = waitForModelingAppId(appId);
        // Wait for 30 seconds before retrieving the result directory
        Thread.sleep(30 * 1000L);
        String resultDir = status.getResultDirectory();

        if (resultDir != null) {
            return UuidUtils.parseUuid(resultDir);
        } else {
            log.warn(String.format("No result directory for modeling job %s", appId));
            System.out.println(String.format("No result directory for modeling job %s", appId));
            return null;
        }
    }

    private JobStatus waitForAppId(String appId) throws Exception {
        return waitForAppId(appId, builder.getRetrieveJobStatusUrl());
    }

    private JobStatus waitForModelingAppId(String appId) throws Exception {
        return waitForAppId(appId, builder.getModelingJobStatusUrl());
    }

    private JobStatus waitForAppId(String appId, String jobStatusUrl) throws Exception {
        JobStatus status;
        int maxTries = 60;
        int i = 0;
        do {
            String url = String.format(modelingServiceHostPort + jobStatusUrl, appId);
            status = restTemplate.getForObject(url, JobStatus.class);
            Thread.sleep(10000L);
            i++;

            if (i == maxTries) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));

        assertEquals(status.getStatus(), FinalApplicationStatus.SUCCEEDED);
        return status;
    }

    private AbstractMap.SimpleEntry<List<String>, List<String>> getTargetAndFeatures() {
        Model model = new Model();
        model.setName("Model-" + System.currentTimeMillis());
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        StringList features = restTemplate.postForObject(modelingServiceHostPort + builder.getRetrieveFeaturesUrl(),
                model, StringList.class);
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
        private String modelingServiceHostPort;
        private String modelingServiceHdfsBaseDir;
        private Configuration yarnConfiguration;
        private String metadataContents;
        private String modelName;

        private String loadSubmissionUrl = "/rest/load";
        private String modelSubmissionUrl = "/rest/submit";
        private String sampleSubmissionUrl = "/rest/createSamples";
        private String profileSubmissionUrl = "/rest/profile";
        private String retrieveFeaturesUrl = "/rest/features";
        private String retrieveJobStatusUrl = "/rest/getJobStatus/%s";
        private String modelingJobStatusUrl = "/rest/getJobStatus/%s";
        private String hdfsDirToSample;

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

        public Builder modelName(String modelName) {
            this.setModelName(modelName);
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

        public String getModelName() {
            return modelName;
        }

        public void setModelName(String modelName) {
            this.modelName = modelName;
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

    }
}
