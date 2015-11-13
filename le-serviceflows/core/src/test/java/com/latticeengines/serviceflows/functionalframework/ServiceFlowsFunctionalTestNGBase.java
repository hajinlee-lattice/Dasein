package com.latticeengines.serviceflows.functionalframework;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeTest;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceflows-context.xml" })
public abstract class ServiceFlowsFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    private ApplicationContext appContext;

    protected Configuration yarnConfiguration = new Configuration();

    protected ServiceFlowsFunctionalTestNGBase() {
        yarnConfiguration.set("fs.defaultFS", "file:///");
    }

    protected abstract String getFlowBeanName();

    protected String getScenarioName() {
        return null;
    }

    private String getDirectory() {
        if (getScenarioName() == null) {
            return getFlowBeanName();
        }
        return Paths.get(getFlowBeanName(), getScenarioName()).toString();
    }

    protected String getIdColumnName(String tableName) {
        return null;
    }

    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }

    @BeforeTest(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, getTargetDirectory());
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/checkpoints");
    }

    protected Table executeDataFlow() throws Exception {
        return executeDataFlow(new DataFlowParameters());
    }

    protected Table executeDataFlow(DataFlowParameters parameters) {
        DataFlowContext context = createDataFlowContext(parameters);
        String beanName = getFlowBeanName();
        return dataTransformationService.executeNamedTransformation(context, beanName);
    }

    protected Map<String, Table> getSources() {
        Map<String, Table> sources = new HashMap<>();

        Map<String, String> sourcePaths = getSourcePaths();
        for (String key : sourcePaths.keySet()) {
            String path = sourcePaths.get(key);
            String idColumn = getIdColumnName(key);
            String lastModifiedColumn = getLastModifiedColumnName(key);
            sources.put(key,
                    MetadataConverter.readMetadataFromAvroFile(yarnConfiguration, path, idColumn, lastModifiedColumn));
        }
        return sources;
    }

    protected Map<String, String> getSourcePaths() {
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources(getDirectory() + "/*/*.avro");
            Map<String, String> sourcePaths = new HashMap<>();
            for (Resource resource : resources) {
                String path = resource.getFile().getAbsolutePath();
                String[] parts = path.split("\\/");
                sourcePaths.put(parts[parts.length - 2], path);
            }
            return sourcePaths;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected DataFlowContext createDataFlowContext(DataFlowParameters parameters) {
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("ENGINE", "TEZ");
        ctx.setProperty("TARGETPATH", getTargetDirectory());
        ctx.setProperty("TARGETTABLENAME", getFlowBeanName());
        ctx.setProperty("FLOWNAME", getFlowBeanName());
        ctx.setProperty("CHECKPOINT", true);
        ctx.setProperty("SOURCETABLES", getSources());
        ctx.setProperty("CUSTOMER", "customer");
        ctx.setProperty("PARAMETERS", parameters);
        return ctx;
    }

    private String getTargetDirectory() {
        return "/tmp/Output/" + getDirectory();
    }

    protected List<GenericRecord> readTable(String avroFile) {
        return AvroUtils.getDataFromGlob(yarnConfiguration, avroFile);
    }

    protected List<GenericRecord> readOutput() {
        return readTable(getTargetDirectory() + "/*.avro");
    }

    protected Table getOutputSchema() {
        return MetadataConverter.readMetadataFromAvroFile( //
                yarnConfiguration, getTargetDirectory() + "/*.avro", null, null);
    }

    protected List<GenericRecord> readInput(String source) {
        Map<String, String> paths = getSourcePaths();
        for (String key : paths.keySet()) {
            if (key.equals(source)) {
                return AvroUtils.getDataFromGlob(yarnConfiguration, paths.get(key));
            }
        }
        throw new RuntimeException(String.format("Could not find source with name %s", source));
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

    protected boolean identicalSets(List<GenericRecord> left, String leftId, List<GenericRecord> right, String rightId) {
        return allEntriesExist(left, leftId, right, rightId) && allEntriesExist(right, rightId, left, leftId);
    }

    @SuppressWarnings("unchecked")
    protected <T> Map<T, Integer> histogram(List<GenericRecord> records, String column) {
        Map<T, Integer> results = new HashMap<>();
        for (GenericRecord record : records) {
            T value = (T) record.get(column);
            Integer count = results.get(value);
            if (count == null) {
                results.put(value, 1);
            } else {
                results.put(value, count + 1);
            }
        }

        return results;
    }
}
