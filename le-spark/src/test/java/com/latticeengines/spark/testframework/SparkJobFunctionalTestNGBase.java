package com.latticeengines.spark.testframework;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import com.latticeengines.common.exposed.csv.LECSVFormat;
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
import com.latticeengines.spark.service.impl.LivyServerManager;

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

    @Inject
    private LivyServerManager livyServerManager;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${common.le.stack}")
    protected String leStack;

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
    private Map<String, DataUnit> inputUnits = new ConcurrentHashMap<>();

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
        session = sessionService.startSession(this.getClass().getSimpleName(), //
                Collections.emptyMap(), Collections.emptyMap());
    }

    @SuppressWarnings("unused")
    protected void reuseLivyEnvironment(int sessionId) {
        String livyHost;
        if (Boolean.TRUE.equals(useEmr)) {
            livyHost = livyServerManager.getLivyHost();
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
        jobConfig.setInput(getInputUnitValues());
        return sparkJobService.runJob(session, jobClz, jobConfig);
    }

    // If single test needs to run multiple jobs with different inputs (to cover
    // different test cases), pass in orderedInput and workspace to start jobs
    protected <J extends AbstractSparkJob<C>, C extends SparkJobConfig> //
    SparkJobResult runSparkJob(Class<J> jobClz, C jobConfig, List<String> orderedInput, String workspace) {
        initializeScenario();
        jobConfig.setWorkspace(workspace);
        jobConfig.setInput(getInputUnits(orderedInput));
        return sparkJobService.runJob(session, jobClz, jobConfig);
    }

    protected SparkJobResult runSparkScript(SparkScript script, ScriptJobConfig jobConfig) {
        initializeScenario();
        jobConfig.setWorkspace(getWorkspace());
        jobConfig.setInput(getInputUnitValues());
        return sparkJobService.runScript(session, script, jobConfig);
    }

    protected void verifyResult(SparkJobResult result) {
        List<Function<HdfsDataUnit, Boolean>> verifiers = getTargetVerifiers();
        verify(result, verifiers);
    }

    protected void verifyResult(SparkJobResult result, List<List<Pair<List<String>, Boolean>>> expectedColumns) {
        List<BiFunction<HdfsDataUnit, List<Pair<List<String>, Boolean>>, Boolean>> verifiers = getTargetVerifiers(
                expectedColumns);
        verify(result, expectedColumns, verifiers);
    }

    // If single test needs to run multiple jobs with different inputs (to cover
    // different test cases), pass in verifiers to verify each job's result
    protected void verify(SparkJobResult result, List<Function<HdfsDataUnit, Boolean>> verifiers) {
        verifyOutput(result.getOutput());

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

    protected void verify(SparkJobResult result, List<List<Pair<List<String>, Boolean>>> expectedColumns,
            List<BiFunction<HdfsDataUnit, List<Pair<List<String>, Boolean>>, Boolean>> verifiers) {
        verifyOutput(result.getOutput());

        int numTargets = CollectionUtils.size(result.getTargets());
        int numVerifiers = CollectionUtils.size(verifiers);
        Assert.assertEquals(numTargets, numVerifiers, //
                String.format("There are %d targets but %d verifiers", numTargets, numVerifiers));

        List<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < numTargets; i++) {
            HdfsDataUnit tgt = result.getTargets().get(i);
            List<Pair<List<String>, Boolean>> cols = expectedColumns.get(i);
            BiFunction<HdfsDataUnit, List<Pair<List<String>, Boolean>>, Boolean> verifier = verifiers.get(i);
            runnables.add(() -> verifier.apply(tgt, cols));
        }

        if (CollectionUtils.size(runnables) > 1) {
            int poolSize = Math.min(4, numTargets);
            ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("spark-job-verifier", poolSize);
            ThreadPoolUtils.runRunnablesInParallel(executors, runnables, 30, 5);
        } else if (CollectionUtils.size(runnables) == 1) {
            runnables.get(0).run();
        }
    }

    private List<DataUnit> getInputUnitValues() {
        List<String> orderedInput = getInputOrder();
        return getInputUnits(orderedInput);
    }

    private List<DataUnit> getInputUnits(List<String> orderedInput) {
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
        putHdfsDataUnit(recordName, dirPath, DataUnit.DataFormat.AVRO);
        return recordName;
    }

    private void copyCSVDataToHdfs(String dirPath, String fileName, String[] headers, Object[] values)
            throws IOException {
        try (FileSystem fs = FileSystem.newInstance(yarnConfiguration)) {
            try (OutputStream outputStream = fs.create(new Path(dirPath + File.separator + fileName))) {
                try (CSVPrinter csvPrinter = new CSVPrinter(
                        new OutputStreamWriter(outputStream, StandardCharsets.UTF_8),
                        LECSVFormat.format.withHeader(headers))) {
                    csvPrinter.printRecord(values);
                }
            }
        }
    }

    private void putHdfsDataUnit(String recordName, String dirPath, DataUnit.DataFormat dataFormat) {
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setName(recordName);
        dataUnit.setPath(dirPath);
        dataUnit.setDataFormat(dataFormat);
        inputUnits.put(recordName, dataUnit);
    }

    protected void uploadHdfsDataUnitWithCSVFmt(String[] headers, Object[][] values) {
        try {
            int seq = inputSeq.getAndIncrement();
            String recordName = "Input" + seq;
            String dirPath = getWorkspace() + "/" + recordName;
            if (HdfsUtils.fileExists(yarnConfiguration, dirPath)) {
                HdfsUtils.rmdir(yarnConfiguration, dirPath);
            }
            int index = 0;
            for (Object[] value : values) {
                String fileName = recordName + "-" + (index++) + ".csv";
                copyCSVDataToHdfs(dirPath, fileName, headers, value);
            }
            putHdfsDataUnit(recordName, dirPath, DataUnit.DataFormat.CSV);
        } catch (Exception e) {
            throw new RuntimeException("Failed to upload csv to hdfs.", e);
        }
    }

    protected Iterator<GenericRecord> verifyAndReadTarget(HdfsDataUnit target) {
        String path = target.getPath();
        try {
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, path));
            if (CollectionUtils.isNotEmpty(target.getPartitionKeys())) {
                path = path + StringUtils.repeat("/**", target.getPartitionKeys().size());
            }
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

    protected List<BiFunction<HdfsDataUnit, List<Pair<List<String>, Boolean>>, Boolean>> getTargetVerifiers(
            List<List<Pair<List<String>, Boolean>>> expectedColumns) {
        List<BiFunction<HdfsDataUnit, List<Pair<List<String>, Boolean>>, Boolean>> list = new ArrayList<>();
        for (int i = 0; i < expectedColumns.size(); i++) {
            list.add(this::verifyMultipleTarget);
        }
        return list;
    }

    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        return true;
    }

    protected List<String> hdfsOutputsAsInputs(List<HdfsDataUnit> units) {
        inputUnits = new HashMap<>();
        List<String> names = new ArrayList<>();
        units.forEach(unit -> {
            int seq = inputSeq.getAndIncrement();
            String recordName = "Input" + seq;
            String dirPath = getWorkspace() + "/" + recordName;
            try {
                HdfsUtils.fileExists(yarnConfiguration, unit.getPath());
            } catch (Exception e) {
                throw new RuntimeException("Partitioned output path don't exist.", e);
            }
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, dirPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, dirPath);
                }
                HdfsUtils.copyFiles(yarnConfiguration, unit.getPath(), dirPath);
                HdfsUtils.rmdir(yarnConfiguration, dirPath + "/" + "_SUCCESS");
            } catch (Exception e) {
                throw new RuntimeException("Partition copy failed.", e);
            }
            HdfsDataUnit dataUnit = new HdfsDataUnit();
            dataUnit.setName(recordName);
            dataUnit.setPath(dirPath);
            dataUnit.setPartitionKeys(unit.getPartitionKeys());
            inputUnits.put(recordName, dataUnit);
            names.add(recordName);
        });
        return names;
    }

    protected Boolean verifyMultipleTarget(HdfsDataUnit tgt,
            List<Pair<List<String>, Boolean>> accountAndContactExpectedCols) {
        return true;
    }

    public Map<String, DataUnit> getInputUnits() {
        return this.inputUnits;
    }

    public void setInputUnits(Map<String, DataUnit> inputUnits) {
        this.inputUnits = inputUnits;
    }

    protected String debugStr(GenericRecord record, Collection<String> fields) {
        List<String> nameValues = fields.stream() //
                .map(field -> String.format("%s=%s", field, record.get(field))) //
                .collect(Collectors.toList());
        return String.join(",", nameValues);
    }

}
