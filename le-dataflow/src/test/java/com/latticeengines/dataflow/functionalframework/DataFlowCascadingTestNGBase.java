package com.latticeengines.dataflow.functionalframework;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:dataflow-context.xml" })
public abstract class DataFlowCascadingTestNGBase extends AbstractTestNGSpringContextTests {
    private boolean local = getenv("SERVICEFLOWS_LOCAL", true, Boolean.class);
    private String engine = getenv("SERVICEFLOWS_ENGINE", "FLINK", String.class);
    protected static final String AVRO_INPUT = "AvroInput";
    protected static final String AVRO_DIR = "/tmp/avro";

    private static final Logger log = LoggerFactory.getLogger(DataFlowCascadingTestNGBase.class);

    @Autowired
    private DataTransformationService dataTransformationService;

    protected Configuration yarnConfiguration = new Configuration();
    protected Map<String, String> sourcePaths = new HashMap<>();
    protected Map<String, String> templatePaths = new HashMap<>();

    protected abstract String getFlowBeanName();

    protected String getScenarioName() {
        return null;
    }

    protected String getDirectory() {
        if (getScenarioName() == null) {
            return getFlowBeanName();
        }
        return Paths.get(getFlowBeanName(), getScenarioName()).toString();
    }

    protected void postProcessSourceTable(Table table) {
    }

    protected String getIdColumnName(String tableName) {
        return null;
    }

    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }

    @BeforeTest(groups = "functional")
    public void setup() throws Exception {
        if (local) {
            yarnConfiguration.set("fs.defaultFS", "file:///");
            log.info("Running locally");
        } else {
            log.info("Running in hadoop cluster");
            if (yarnConfiguration.get("fs.defaultFS").equals("file:///")) {
                throw new RuntimeException(
                        "fs.defaultFS is still set to file:///.  $HADOOP_HOME/etc/hadoop is likely not in the classpath");
            }
        }

        HdfsUtils.rmdir(yarnConfiguration, getTargetDirectory());
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/checkpoints");
        HdfsUtils.rmdir(yarnConfiguration, AVRO_DIR);

    }

    protected void initialize() {
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] sourceResources = new Resource[] {};
            try {
                sourceResources = resolver.getResources(getDirectory() + "/*/*.avro");
                log.info("Resolved resources for " + getDirectory());
            } catch (Exception e) {
                log.warn("Cannot resolve resource for " + getDirectory());
            }

            sourcePaths = new HashMap<>();
            for (Resource resource : sourceResources) {
                String path = resource.getFile().getAbsolutePath();
                String[] parts = path.split("\\/");
                if (parts[parts.length - 1].contains(".avro")) {
                    path = path.substring(0, path.lastIndexOf("/"));
                    path += "/*.avro";
                }
                sourcePaths.put(parts[parts.length - 2], path);
            }
            if (!local) {
                List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();
                Map<String, String> modifiedSourcePaths = new HashMap<>();
                for (String key : sourcePaths.keySet()) {
                    String hdfsDestination = AVRO_DIR + "/" + key + ".avro";
                    entries.add(new AbstractMap.SimpleEntry<>(sourcePaths.get(key), hdfsDestination));
                    modifiedSourcePaths.put(key, hdfsDestination);
                }
                copy(entries);
                sourcePaths = modifiedSourcePaths;
            }
            if (HdfsUtils.fileExists(yarnConfiguration, AVRO_DIR + "/" + AVRO_INPUT + ".avro")) {
                sourcePaths.put(AVRO_INPUT, AVRO_DIR + "/" + AVRO_INPUT + ".avro");
            }
            Map<String, String> extraSourcePaths = extraSourcePaths();
            extraSourcePaths.forEach((n, p) -> sourcePaths.put(n, p));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] templateResources = new Resource[] {};
            try {
                templateResources = resolver.getResources(getDirectory() + "/*/*.json");
            } catch (Exception e) {
                log.warn("Cannot resolve template for " + getDirectory());
            }

            templatePaths = new HashMap<>();
            for (Resource resource : templateResources) {
                String path = resource.getFile().getAbsolutePath();
                String[] parts = path.split("\\/");
                templatePaths.put(parts[parts.length - 2], path);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Map<String, String> extraSourcePaths() {
        return Collections.emptyMap();
    }

    protected Table executeDataFlow() {
        initialize();
        return executeDataFlow(new DataFlowParameters());
    }

    protected Table executeDataFlow(DataFlowParameters parameters) {
        initialize();
        DataFlowContext context = createDataFlowContext(parameters);
        String beanName = getFlowBeanName();
        return dataTransformationService.executeNamedTransformation(context, beanName);
    }

    protected Map<String, Table> getSources() {
        Map<String, Table> sources = new HashMap<>();

        for (String key : sourcePaths.keySet()) {
            String path = sourcePaths.get(key);
            String template = templatePaths.get(key);
            Table table;
            if (template != null) {
                table = retrieveSourceTableTemplate(key, path);
            } else {
                String idColumn = getIdColumnName(key);
                String lastModifiedColumn = getLastModifiedColumnName(key);
                table = MetadataConverter.getTable(yarnConfiguration, path, idColumn, lastModifiedColumn);
            }
            postProcessSourceTable(table);
            sources.put(key, table);
        }
        return sources;
    }

    private Table retrieveSourceTableTemplate(String name, String extractPath) {
        if (templatePaths.containsKey(name)) {
            try (FileInputStream fis = new FileInputStream(templatePaths.get(name))) {
                Table table = JsonUtils.deserialize(IOUtils.toString(fis, Charset.defaultCharset()), Table.class);
                fixExtractPaths(table, extractPath);
                return table;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException(String.format("Could not find table template for source with name %s", name));
        }
    }

    private void fixExtractPaths(Table table, String extractPath) {
        while (table.getExtracts().size() > 1) {
            table.getExtracts().remove(1);
        }
        table.getExtracts().get(0).setPath(extractPath);
    }

    private void copy(List<AbstractMap.SimpleEntry<String, String>> entries) {
        try {
            for (AbstractMap.SimpleEntry<String, String> e : entries) {
                HdfsUtils.copyLocalToHdfs(yarnConfiguration, e.getKey(), e.getValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Path getDestinationPath(String destPath, Resource res) throws IOException {
        Path dest = new Path(destPath, res.getFilename());
        return dest;
    }

    protected String getExecutionEngine() {
        return engine;
    }

    protected DataFlowContext createDataFlowContext(DataFlowParameters parameters) {
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.HADOOPCONF, yarnConfiguration);
        ctx.setProperty(DataFlowProperty.ENGINE, getExecutionEngine());
        ctx.setProperty(DataFlowProperty.TARGETPATH, getTargetDirectory());
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, getFlowBeanName());
        ctx.setProperty(DataFlowProperty.FLOWNAME, getFlowBeanName());
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.SOURCETABLES, getSources());
        ctx.setProperty(DataFlowProperty.CUSTOMER, "customer");
        ctx.setProperty(DataFlowProperty.PARAMETERS, parameters);
        ctx.setProperty(DataFlowProperty.DEBUG, false);
        return updateDataFlowContext(ctx);
    }

    protected DataFlowContext updateDataFlowContext(DataFlowContext ctx) {
        return ctx;
    }

    protected String getTargetDirectory() {
        return "/tmp/DataFlowsFunctionalTest/" + getDirectory();
    }

    protected List<GenericRecord> readTable(String avroFile) {
        return AvroUtils.getDataFromGlob(yarnConfiguration, avroFile);
    }

    protected List<GenericRecord> readOutput() {
        return readTable(getTargetDirectory() + "/*.avro");
    }

    protected Table getOutputSchema() {
        return MetadataConverter.getTable( //
                yarnConfiguration, getTargetDirectory() + "/*.avro", null, null);
    }

    protected List<GenericRecord> readInput(String source) {
        Map<String, String> paths = sourcePaths;
        for (String key : paths.keySet()) {
            if (key.equals(source)) {
                log.info(String.format("Table Name = %s, Path = %s", key, paths.get(key)));
                return AvroUtils.getDataFromGlob(yarnConfiguration, paths.get(key));
            }
        }
        throw new RuntimeException(String.format("Could not find source with name %s", source));
    }

    protected void uploadDataToSharedAvroInput(Object[][] data, List<Pair<String, Class<?>>> columns) {
        uploadAvro(data, columns, AVRO_INPUT, AVRO_DIR);
    }

    protected void uploadAvro(Object[][] data, List<Pair<String, Class<?>>> columns, String recordName,
            String dirPath) {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            schemaMap.put(columns.get(i).getKey(), columns.get(i).getValue());
        }
        Schema schema = AvroUtils.constructSchema(recordName, schemaMap);
        List<GenericRecord> records = new ArrayList<>();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Object[] tuple : data) {
            for (int i = 0; i < columns.size(); i++) {
                builder.set(columns.get(i).getKey(), tuple[i]);
            }
            records.add(builder.build());
        }
        String fileName = recordName + ".avro";
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, dirPath)) {
                HdfsUtils.rmdir(yarnConfiguration, dirPath);
            }
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, dirPath + File.separator + fileName, records);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }
    }

    protected void copyAvro(String localFile, String recordName, String dirPath) {
        String fileName = recordName + ".avro";
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, dirPath)) {
                HdfsUtils.rmdir(yarnConfiguration, dirPath);
            }
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, localFile, dirPath + File.separator + fileName);

        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }
    }

    protected void copyAvroDir(String localPath, String hdfsPath) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
            }
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, localPath, hdfsPath);
        } catch (Exception e) {
            Assert.fail("Failed to upload local path " + localPath, e);
        }
    }

    /**
     * Returns whether everything in test with testId exists in master using
     * masterId. Duplicates are ignored. Can't we just upgrade to Java 8
     * already?
     */
    protected boolean allEntriesExist( //
            final List<GenericRecord> test, //
            final String testId, //
            final List<GenericRecord> master, //
            final String masterId) {

        return Iterables.all(test, new Predicate<GenericRecord>() {

            @Override
            public boolean apply(final GenericRecord testRecord) {
                return Iterables.any(master, new Predicate<GenericRecord>() {

                    @Override
                    public boolean apply(GenericRecord masterRecord) {
                        if (masterRecord.get(masterId) == null && testRecord.get(testId) == null) {
                            return true;
                        }
                        if (masterRecord.get(masterId) == null || testRecord.get(testId) == null) {
                            return false;
                        }
                        return masterRecord.get(masterId).equals(testRecord.get(testId));
                    }
                });
            }
        });
    }

    protected boolean containsValues(final List<GenericRecord> records, final String column, final String... values) {
        return Iterables.any(records, new Predicate<GenericRecord>() {

            @Override
            public boolean apply(final GenericRecord record) {
                return Iterables.any(Arrays.asList(values), new Predicate<String>() {

                    @Override
                    public boolean apply(final String value) {
                        return record.get(column).toString().equals(value);
                    }
                });

            }
        });
    }

    @SuppressWarnings("unchecked")
    protected <T> boolean allValuesEqual(final List<GenericRecord> records, final String column, final T value) {
        return Iterables.all(records, new Predicate<GenericRecord>() {

            @Override
            public boolean apply(GenericRecord record) {
                T casted = (T) record.get(column);
                return casted.equals(value);
            }
        });
    }

    protected boolean identicalSets(List<GenericRecord> left, String leftId, List<GenericRecord> right,
            String rightId) {
        return allEntriesExist(left, leftId, right, rightId) && allEntriesExist(right, rightId, left, leftId);
    }

    protected Map<Object, Integer> histogram(List<GenericRecord> records, final String column) {
        return histogram(records, new Function<GenericRecord, Object>() {

            @Nullable
            @Override
            public Object apply(@Nullable GenericRecord input) {
                return input.get(column);
            }
        });
    }

    protected Map<Object, Integer> histogram(List<GenericRecord> records, Function<GenericRecord, Object> keyProvider) {
        Map<Object, Integer> results = new HashMap<>();
        for (GenericRecord record : records) {
            Object key = keyProvider.apply(record);
            Integer count = results.get(key);
            if (count == null) {
                results.put(key, 1);
            } else {
                results.put(key, count + 1);
            }
        }

        return results;
    }

    private <T> T getenv(String variable, T dflt, Class<T> clazz) {
        String value = System.getenv(variable);
        log.info(variable + ": " + value);
        if (value == null) {
            return dflt;
        }
        try {
            return clazz.getConstructor(new Class[] { String.class }).newInstance(value);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to parse %s as a %s", value, clazz.getSimpleName()));
        }
    }

    protected void setEngine(String engine) {
        this.engine = engine;
    }
}
