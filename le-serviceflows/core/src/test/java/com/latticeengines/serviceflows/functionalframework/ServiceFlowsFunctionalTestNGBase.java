package com.latticeengines.serviceflows.functionalframework;

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
import org.testng.annotations.BeforeClass;

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
public abstract class ServiceFlowsFunctionalTestNGBase<T extends DataFlowParameters> extends
        AbstractTestNGSpringContextTests {

    @Autowired
    private DataTransformationService dataTransformationService;

    @Autowired
    private ApplicationContext appContext;

    protected Configuration yarnConfiguration = new Configuration();

    protected ServiceFlowsFunctionalTestNGBase() {
        yarnConfiguration.set("fs.defaultFS", "file:///");
    }

    protected abstract String getFlowBeanName();

    protected String getIdColumnName(String tableName) {
        return null;
    }

    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }

    protected DataFlowParameters getDataFlowParameters() {
        return null;
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, getTargetDirectory());
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/checkpoints");
    }

    protected Table executeDataFlow() throws Exception {
        DataFlowContext context = createDataFlowContext();
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
            Resource[] resources = resolver.getResources(getFlowBeanName() + "/*/*.avro");
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

    protected DataFlowContext createDataFlowContext() {
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("ENGINE", "MR");
        ctx.setProperty("TARGETPATH", getTargetDirectory());
        ctx.setProperty("TARGETTABLENAME", getFlowBeanName());
        ctx.setProperty("FLOWNAME", getFlowBeanName());
        ctx.setProperty("CHECKPOINT", true);
        ctx.setProperty("SOURCETABLES", getSources());
        ctx.setProperty("CUSTOMER", "customer");
        ctx.setProperty("PARAMETERS", getDataFlowParameters());
        return ctx;
    }

    private String getTargetDirectory() {
        return "/tmp/Output/" + getFlowBeanName();
    }

    protected List<GenericRecord> readTable(String avroFile) {
        return AvroUtils.getDataFromGlob(yarnConfiguration, avroFile);
    }

    protected List<GenericRecord> readOutput() {
        return readTable(getTargetDirectory() + "/*.avro");
    }

    /**
     * Returns whether everything in test with testId exists in master using
     * masterId. Duplicates are ignored. Can't we just upgrade to Java 8
     * already?
     */
    protected boolean allEntriesExist(List<GenericRecord> test, String testId, List<GenericRecord> master,
            String masterId) {

        for (GenericRecord record : test) {
            Object value = record.get(testId);
            boolean found = false;
            for (GenericRecord inner : master) {
                Object innerValue = inner.get(masterId);
                if (value.equals(innerValue)) {
                    found = true;
                    break;
                }
            }
            if (found == false) {
                return false;
            }
        }

        return true;
    }

    protected boolean identicalSets(List<GenericRecord> left, String leftId, List<GenericRecord> right, String rightId) {
        return allEntriesExist(left, leftId, right, rightId) && allEntriesExist(right, rightId, left, leftId);
    }
}
