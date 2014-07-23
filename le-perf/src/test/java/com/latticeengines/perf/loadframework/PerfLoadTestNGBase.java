package com.latticeengines.perf.loadframework;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingElement;
import com.latticeengines.domain.exposed.dataplatform.algorithm.RandomForestAlgorithm;
import com.latticeengines.perf.job.configuration.ConstructModelConfiguration;
import com.latticeengines.perf.job.configuration.OnBoardConfiguration;
import com.latticeengines.perf.job.runnable.impl.Profile;

public class PerfLoadTestNGBase {
    protected static final Log log = LogFactory.getLog(PerfLoadTestNGBase.class);

    protected Properties prop;

    protected FileSystem fs;

    protected ModelDefinition modelDef;

    protected Model model;

    protected String customerBaseDir;

    protected String restEndpointHost;

    protected int numOfThreads;

    protected int numOfCustomers;

    protected int numOfRuns;

    protected ExecutorService executor;

    @Parameters({ "dataplatformProp", "apiProp", "numOfThreads", "numOfCustomers", "numOfRuns" })
    @BeforeClass(groups = "load")
    public void setup(String dataplatformProp, String apiProp, String numOfThreads, String numOfCustomers,
            String numOfRuns) {
        this.numOfThreads = Integer.parseInt(numOfThreads);
        this.numOfCustomers = Integer.parseInt(numOfCustomers);
        this.numOfRuns = Integer.parseInt(numOfRuns);
        executor = Executors.newFixedThreadPool(this.numOfThreads);

        prop = generateProperty(apiProp);
        restEndpointHost = prop.getProperty("api.rest.endpoint.hostport");

        prop = generateProperty(dataplatformProp);
        customerBaseDir = prop.getProperty("dataplatform.customer.basedir");
        YarnConfiguration yarnConfiguration = createYarnConfiguration(prop);

        try {
            fs = FileSystem.get(yarnConfiguration);
        } catch (IOException e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
        }
        modelDef = produceModelDef(0);
    }

    protected Properties generateProperty(String propertyPath) {
        Properties prop = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(new File(propertyPath));
            prop.load(inputStream);
        } catch (FileNotFoundException e) {
            log.error("property file '" + propertyPath + "' not found in the classpath");
            log.error(ExceptionUtils.getFullStackTrace(e));
        } catch (IOException e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error(ExceptionUtils.getFullStackTrace(e));
            }
        }
        return prop;
    }

    protected YarnConfiguration createYarnConfiguration(Properties prop) {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("fs.defaultFS", "hdfs://bodcprodvhdp195.prod.lattice.local:8020");
        yarnConfiguration.set("yarn.resourcemanager.address",
                prop.getProperty("dataplatform.yarn.resourcemanager.address"));
        yarnConfiguration.set("yarn.resourcemanager.scheduler.address",
                prop.getProperty("dataplatform.yarn.resourcemanager.scheduler.address"));
        yarnConfiguration.set("yarn.resourcemanager.webapp.address",
                prop.getProperty("dataplatform.yarn.resourcemanager.webapp.address"));
        yarnConfiguration.set("yarn.nodemanager.remote-app-log-dir",
                prop.getProperty("dataplatform.yarn.nodemanager.remote-app-log-dir"));
        return yarnConfiguration;
    }

    protected Model produceAModel(String customer) throws Exception {
        Model model = new Model();
        model.setCustomer(customer);
        model.setModelDefinition(modelDef);
        model.setName("Nutanix Random Forest Model on Depivoted Data");
        model.setTable("Q_EventTable_Nutanix");
        model.setMetadataTable("EventMetadata");
        model.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        model.setDataFormat("avro");
        model.setTargetsList(Arrays.<String> asList(new String[] { "P1_Event" }));

        return model;
    }

    protected ModelDefinition produceModelDef(int priority) {
        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(priority);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("all");
        randomForestAlgorithm
                .setAlgorithmProperties("criterion=gini n_estimators=10 n_jobs=4 min_samples_split=25 min_samples_leaf=10 bootstrap=True");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Load Test");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm }));
        return modelDef;
    }

    protected LoadConfiguration createLoadConfiguration(String customer) throws Exception{
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();

        // dataplatform.dlorchestration.datasource.host=10.41.1.250
        // dataplatform.dlorchestration.datasource.port=1433
        // dataplatform.dlorchestration.datasource.dbname=LeadScoringDB_buildmachine

        // String host =
        // prop.getProperty("dataplatform.dlorchestration.datasource.host");
        String host = "10.41.1.250";
        // int port =
        // Integer.parseInt(prop.getProperty("dataplatform.dlorchestration.datasource.port"));
        int port = 1433;
        // String database =
        // prop.getProperty("dataplatform.dlorchestration.datasource.dbname");
        String database = "LeadScoringDB_qe";

        String user = prop.getProperty("dataplatform.dlorchestration.datasource.user");
        String password = CipherUtils.decrypt(prop
                .getProperty("dataplatform.dlorchestration.datasource.password.encrypted"));
        builder.host(host).port(port).db(database).user(user).password(password);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer(customer);
        config.setTable("Q_EventTable_Nutanix");
        config.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        return config;
    }

    protected SamplingConfiguration createSamplingConfiguration(String customer) {
        SamplingConfiguration config = new SamplingConfiguration();
        config.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        config.addSamplingElement(s0);
        config.addSamplingElement(s1);
        config.addSamplingElement(all);
        config.setCustomer(customer);
        config.setTable(this.model.getTable());
        return config;
    }

    protected DataProfileConfiguration createDataProfileConfiguration(String customer) {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(customer);
        config.setTable(this.model.getTable());
        config.setMetadataTable(this.model.getMetadataTable());
        config.setSamplePrefix("all");
        config.setExcludeColumnList(Profile.createExcludeList());
        return config;
    }

    protected OnBoardConfiguration createOnBoardConfiguration(String customer) throws Exception {
        model = produceAModel(customer);
        LoadConfiguration lc = createLoadConfiguration(customer);
        lc.setCustomer(customer);
        SamplingConfiguration sc = createSamplingConfiguration(customer);
        sc.setCustomer(customer);
        DataProfileConfiguration dc = createDataProfileConfiguration(customer);
        dc.setCustomer(customer);
        return new OnBoardConfiguration(lc, sc, dc);
    }

    protected ConstructModelConfiguration createConstructModelConfiguration(String customer) throws Exception {
        OnBoardConfiguration obc = createOnBoardConfiguration(customer);
        return new ConstructModelConfiguration(obc, model);
    }
}
