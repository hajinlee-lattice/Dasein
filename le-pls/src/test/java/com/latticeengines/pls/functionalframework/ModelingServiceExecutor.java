package com.latticeengines.pls.functionalframework;

import static org.testng.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
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

public class ModelingServiceExecutor {

    private Builder builder;

    private String modelingServiceHostPort;

    private String modelingServiceHdfsBaseDir;

    private RestTemplate restTemplate = new RestTemplate();

    private Configuration yarnConfiguration;
    
    private static int modelCount = 1;

    public ModelingServiceExecutor(Builder builder) {
        this.builder = builder;
        this.modelingServiceHostPort = builder.getModelingServiceHostPort();
        this.modelingServiceHdfsBaseDir = builder.getModelingServiceHdfsBaseDir();
        this.yarnConfiguration = builder.getYarnConfiguration();
    }

    public void init() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(String.format("%s/%s", modelingServiceHdfsBaseDir, builder.getCustomer())), true);
    }

    public void runPipeline() throws Exception {
        writeMetadataFile();
        loadData();
        sample();
        profile();
        model();
    }

    private void writeMetadataFile() throws Exception {
        String hdfsPath = String.format("%s/%s/data/%s/metadata.avsc", modelingServiceHdfsBaseDir,
                builder.getCustomer(), builder.getMetadataTable());
        HdfsUtils.writeToFile(yarnConfiguration, hdfsPath, builder.getMetadataContents());
    }

    private void loadData() throws Exception {
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
        config.setKeyCols(Arrays.<String> asList(new String[] { builder.getKeyColumn() }));
        AppSubmission submission = restTemplate.postForObject(modelingServiceHostPort + "/rest/load", config,
                AppSubmission.class, new Object[] {});
        String appId = submission.getApplicationIds().get(0);
        waitForAppId(appId);
    }
    
    private void sample() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        samplingConfig.addSamplingElement(all);
        samplingConfig.setCustomer(builder.getCustomer());
        samplingConfig.setTable(builder.getTable());
        AppSubmission submission = restTemplate.postForObject(modelingServiceHostPort + "/rest/createSamples",
                samplingConfig, AppSubmission.class, new Object[] {});
        String appId = submission.getApplicationIds().get(0);
        waitForAppId(appId);
    }

    private void profile() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(builder.getCustomer());
        config.setTable(builder.getTable());
        config.setMetadataTable(builder.getMetadataTable());
        config.setExcludeColumnList(Arrays.<String>asList(builder.getProfileExcludeList()));
        config.setSamplePrefix("all");
        config.setTargets(Arrays.<String>asList(builder.getTargets()));
        AppSubmission submission = restTemplate.postForObject(modelingServiceHostPort + "/rest/profile", config,
                AppSubmission.class, new Object[] {});
        String appId = submission.getApplicationIds().get(0);
        waitForAppId(appId);
    }
    
    private void model() throws Exception {
        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Random Forest against all");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm }));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName(builder.getModelName());
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        model.setKeyCols(Arrays.<String> asList(new String[] { builder.getKeyColumn() }));
        model.setDataFormat("avro");
        
        AbstractMap.SimpleEntry<List<String>, List<String>> targetAndFeatures = getTargetAndFeatures();
        model.setTargetsList(targetAndFeatures.getKey());
        model.setFeaturesList(targetAndFeatures.getValue());
        
        AppSubmission submission = restTemplate.postForObject(modelingServiceHostPort + "/rest/submit", model,
                AppSubmission.class, new Object[] {});
        String appId = submission.getApplicationIds().get(0);
        waitForAppId(appId);
    }
    
    private void waitForAppId(String appId) throws Exception {
        JobStatus status = null;
        int maxTries = 60;
        int i = 0;
        do {
            String url = String.format(modelingServiceHostPort + "/rest/getJobStatus/%s", appId);
            status = restTemplate.getForObject(url, JobStatus.class);
            Thread.sleep(10000L);
            i++;
            
            if (i == maxTries) {
                break;
            }
        } while (status.getStatus() != FinalApplicationStatus.SUCCEEDED
                && status.getStatus() != FinalApplicationStatus.FAILED);
        
        assertEquals(status.getStatus(), FinalApplicationStatus.SUCCEEDED);
    }
    
    private AbstractMap.SimpleEntry<List<String>, List<String>> getTargetAndFeatures() {
        Model model = new Model();
        model.setName("Model-" + System.currentTimeMillis());
        model.setTable(builder.getTable());
        model.setMetadataTable(builder.getMetadataTable());
        model.setCustomer(builder.getCustomer());
        StringList features = restTemplate.postForObject(modelingServiceHostPort + "/rest/features", model,
                StringList.class, new Object[] {});
        return new AbstractMap.SimpleEntry<>(Arrays.<String>asList(builder.getTargets()), features.getElements());
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

    }
}
