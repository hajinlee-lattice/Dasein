package com.latticeengines.spark.testframework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.SparkScript;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.service.LivySessionService;
import com.latticeengines.spark.exposed.service.SparkJobService;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-spark-context.xml" })
public abstract class SparkJobFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(SparkJobFunctionalTestNGBase.class);

    @Inject
    private LivySessionService sessionService;

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private SparkJobService sparkJobService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${dataflowapi.spark.driver.cores}")
    private String driverCores;

    @Value("${dataflowapi.spark.driver.mem}")
    private String driverMem;

    @Value("${dataflowapi.spark.executor.cores}")
    private String executorCores;

    @Value("${dataflowapi.spark.executor.mem}")
    private String executorMem;

    @Value("${dataflowapi.spark.max.executors}")
    private String maxExecutors;

    protected LivySession session;
    private AtomicInteger inputSeq = new AtomicInteger(0);
    private Map<String, DataUnit> inputUnits = new HashMap<>();

    protected String getJobName() {
        return null;
    }

    protected String getScenarioName() {
        return null;
    }

    // override this to enforce ordering between inputs
    protected List<String> getInputOrder() {
        return null;
    }

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
        // reuseLivyEnvironment(7);
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
        tearDownLivyEnvironment();
    }

    protected void setupLivyEnvironment() {
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = emrCacheService.getLivyUrl();
        } else {
            livyHost = "http://localhost:8998";
        }
        session = sessionService.startSession(livyHost, this.getClass().getSimpleName(), //
                Collections.emptyMap(), Collections.emptyMap());
    }

    @SuppressWarnings("unused")
    protected void reuseLivyEnvironment(int sessionId) {
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = emrCacheService.getLivyUrl();
        } else {
            livyHost = "http://localhost:8998";
        }
        session = sessionService.getSession(new LivySession(livyHost, sessionId));
    }

    protected void tearDownLivyEnvironment() {
        sessionService.stopSession(session);
    }

    protected String getWorkspace() {
        return String.format("/tmp/%s/%s", leStack, this.getClass().getSimpleName());
    }

    private void initializeScenario() {
        String dataRoot = getJobName();
        if (StringUtils.isNotBlank(dataRoot)) {
            String scenario = getScenarioName();
            if (StringUtils.isNotBlank(scenario)) {
                dataRoot += "/" + scenario;
            }
        }
        if (StringUtils.isNotBlank(dataRoot)) {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = new Resource[] {};
            try {
                resources = resolver.getResources(dataRoot + "/*/*.avro");
                log.info("Resolved resources for " + dataRoot);
            } catch (Exception e) {
                log.error("Cannot resolve resource for " + dataRoot, e);
            }

            Map<String, String> dataSetPaths = new HashMap<>();
            for (Resource resource : resources) {
                String path;
                try {
                    path = resource.getFile().getAbsolutePath();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to get absolute path of resource file.", e);
                }
                String[] parts = path.split("\\/");
                if (parts[parts.length - 1].contains(".avro")) {
                    path = path.substring(0, path.lastIndexOf("/"));
                }
                String dataSetName = parts[parts.length - 2];
                dataSetPaths.put(dataSetName, path);
            }

            String workspace = getWorkspace();
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, workspace)) {
                    HdfsUtils.rmdir(yarnConfiguration, workspace);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to cleanup workspace", e);
            }

            dataSetPaths.forEach((name, localPath) -> {
                String path = workspace + "/" + name;
                try {
                    HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, localPath, path);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy data to hdfs", e);
                }
                HdfsDataUnit dataUnit = new HdfsDataUnit();
                dataUnit.setName(name);
                dataUnit.setPath(path);
                inputUnits.put(name, dataUnit);
            });
        }
    }

    protected <J extends AbstractSparkJob<C>, C extends SparkJobConfig> //
    SparkJobResult runSparkJob(Class<J> jobClz, C jobConfig) {
        initializeScenario();
        jobConfig.setWorkspace(getWorkspace());
        jobConfig.setInput(getInputUnits());
        return sparkJobService.runJob(session, jobClz, jobConfig);
    }

    protected SparkJobResult runSparkScript(SparkScript script, ScriptJobConfig jobConfig) {
        initializeScenario();
        jobConfig.setWorkspace(getWorkspace());
        jobConfig.setInput(getInputUnits());
        return sparkJobService.runScript(session, script, jobConfig);
    }

    protected void verifyResult(SparkJobResult result) {
        verifyOutput(result.getOutput());

        List<Function<HdfsDataUnit, Boolean>> verifiers = getTargetVerifiers();
        int numTargets = CollectionUtils.size(result.getTargets());
        int numVerifiers = CollectionUtils.size(verifiers);
        Assert.assertEquals(numTargets, numVerifiers, //
                String.format("There are %d targets but %d verifiers", numTargets, numVerifiers));

        List<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < numTargets; i++) {
            HdfsDataUnit tgt = result.getTargets().get(i);
            Function<HdfsDataUnit, Boolean> verifier = verifiers.get(i);
            runnables.add(() -> verifier.apply(tgt));
        }

        if (CollectionUtils.size(runnables) > 1) {
            int poolSize = Math.min(4, numTargets);
            ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("spark-job-verifier", poolSize);
            ThreadPoolUtils.runRunnablesInParallel(executors, runnables, 30, 5);
        } else if (CollectionUtils.size(runnables) == 1) {
            runnables.get(0).run();
        }
    }

    private List<DataUnit> getInputUnits() {
        List<String> orderedInput = getInputOrder();
        if (CollectionUtils.isNotEmpty(orderedInput)) {
            return orderedInput.stream().map(name -> {
                if (inputUnits.containsKey(name)) {
                    return inputUnits.get(name);
                } else {
                    throw new IllegalStateException("Cannot find named input " + name);
                }
            }).collect(Collectors.toList());
        } else {
            return new ArrayList<>(inputUnits.values());
        }
    }

    protected String uploadHdfsDataUnit(Object[][] data, List<Pair<String, Class<?>>> columns) {
        int seq = inputSeq.getAndIncrement();
        String recordName = "Input" + seq;
        String dirPath = getWorkspace() + "/" + recordName;
        try {
            AvroUtils.uploadAvro(yarnConfiguration, data, columns, recordName, dirPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload avro to hdfs.", e);
        }
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setName(recordName);
        dataUnit.setPath(dirPath);
        inputUnits.put(recordName, dataUnit);
        return recordName;
    }

    protected Iterator<GenericRecord> verifyAndReadTarget(HdfsDataUnit target) {
        String path = target.getPath();
        try {
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, path));
            if (DataUnit.DataFormat.PARQUET.equals(target.getDataFormat())) {
                log.info("Read parquet files in " + path + " as avro records.");
                return ParquetUtils.iteratorParquetFiles(yarnConfiguration, PathUtils.toParquetGlob(path));
            } else {
                return AvroUtils.iterateAvroFiles(yarnConfiguration, PathUtils.toAvroGlob(path));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void verifyOutput(String output) {
    }

    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.singletonList(this::verifySingleTarget);
    }

    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        return true;
    }

}
