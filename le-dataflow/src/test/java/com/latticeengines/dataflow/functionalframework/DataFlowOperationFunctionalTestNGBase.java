package com.latticeengines.dataflow.functionalframework;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class DataFlowOperationFunctionalTestNGBase extends DataFlowFunctionalTestNGBase {
    
    private static final Logger log = LoggerFactory.getLogger(DataFlowOperationFunctionalTestNGBase.class);

    public final String TARGET_PATH = "/tmp/DataFlowOperationTestOutput";
    public final String INPUT_PATH = "/tmp/DataFlowOperationTestInput";
    protected final String DYNAMIC_SOURCE = "DynamicSource";
    public final boolean LOCAL = true;

    @Autowired
    private DataTransformationService dataTransformationService;
    protected Configuration configuration = new Configuration();

    @BeforeMethod(groups = "functional")
    public void setup() throws Exception {
        if (LOCAL) {
            configuration.set("fs.defaultFS", "file:///");
            configuration.set("fs.default.name", "file:///");
        }

        HdfsUtils.rmdir(configuration, TARGET_PATH);
        HdfsUtils.rmdir(configuration, "/tmp/checkpoints");
        HdfsUtils.rmdir(configuration, INPUT_PATH);

        String lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro")
                .getPath();
        String opportunity = ClassLoader
                .getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        String contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro")
                .getPath();
        String lead2 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead2.avro")
                .getPath();
        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();
        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, INPUT_PATH));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, INPUT_PATH));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, INPUT_PATH));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead2, INPUT_PATH));

        FileSystem fs = FileSystem.get(configuration);
        doCopy(fs, entries);

        LogManager.getLogger(AttributeUtils.class).setLevel(Level.WARN);
    }

    @AfterMethod(groups = "functional")
    public void afterMethod() {
        cleanupDynamicSource();
    }

    protected Map<String, Table> getSources() {
        Map<String, Table> sources = new HashMap<>();

        Map<String, String> sourcePaths = getSourcePaths();
        for (String key : sourcePaths.keySet()) {
            String path = sourcePaths.get(key);
            String idColumn = null;
            String lastModifiedColumn = null;
            try {
                sources.put(key, MetadataConverter.getTable(configuration, path, idColumn, lastModifiedColumn));
            } catch (Exception e) {
                log.warn("Failed to add source " + key + " using avro path " + path);
            }
        }
        return sources;
    }

    protected Map<String, String> getSourcePaths() {
        Map<String, String> sources = new HashMap<>();
        sources.put("Lead", INPUT_PATH + "/Lead.avro");
        sources.put("Opportunity", INPUT_PATH + "/Opportunity.avro");
        sources.put("Contact", INPUT_PATH + "/Contact.avro");
        sources.put("Lead2", INPUT_PATH + "/Lead2.avro");
        sources.put(DYNAMIC_SOURCE, INPUT_PATH + "/" + DYNAMIC_SOURCE);
        return sources;
    }

    protected Schema getOutputSchema() {
        return AvroUtils.getSchemaFromGlob(configuration, TARGET_PATH + "/*.avro");
    }

    protected List<GenericRecord> readOutput() {
        return readOutput(TARGET_PATH);
    }

    protected List<GenericRecord> readOutput(String targetPath) {
        return AvroUtils.getDataFromGlob(configuration, targetPath + "/*.avro");
    }

    protected List<GenericRecord> readInput(String source) {
        Map<String, String> paths = getSourcePaths();
        for (String key : paths.keySet()) {
            if (key.equals(source)) {
                return AvroUtils.getDataFromGlob(configuration, paths.get(key));
            }
        }
        throw new RuntimeException(String.format("Could not find source with name %s", source));
    }

    protected Table execute(DataFlowBuilder builder) {
        return execute(builder, TARGET_PATH);
    }

    protected Table execute(DataFlowBuilder builder, int partitions) {
        return execute(builder, TARGET_PATH, partitions, null);
    }


    protected Table execute(DataFlowBuilder builder, String targetPath, int partitions, Properties properties) {
        builder.setLocal(LOCAL);

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.SOURCETABLES, getSources());
        ctx.setProperty(DataFlowProperty.CUSTOMER, "Customer");
        ctx.setProperty(DataFlowProperty.TARGETPATH, targetPath);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "Output");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "Flow");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.PARTITIONS, partitions);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, configuration);
        ctx.setProperty(DataFlowProperty.ENGINE, "FLINK");

        if (properties != null) {
            ctx.setProperty(DataFlowProperty.JOBPROPERTIES, properties);
        }

        return dataTransformationService.executeNamedTransformation(ctx, builder);
    }

    protected Table execute(DataFlowBuilder builder, String targetPath) {
        return execute(builder, targetPath, 1, null);
    }

    protected void uploadDynamicSourceAvro(Object[][] data, Schema schema) {
        uploadDynamicSourceAvro(data, schema, "1.avro");
    }

    protected void uploadDynamicSourceAvro(Object[][] data, Schema schema, String fileName) {
        List<GenericRecord> records = AvroUtils.convertToRecords(data, schema);
        try {
            if (HdfsUtils.fileExists(configuration, INPUT_PATH + "/" + DYNAMIC_SOURCE + "/" + fileName)) {
                HdfsUtils.rmdir(configuration, INPUT_PATH + "/" + DYNAMIC_SOURCE + "/" + fileName);
            }
            AvroUtils.writeToHdfsFile(configuration, schema, INPUT_PATH + "/" + DYNAMIC_SOURCE + "/" + fileName, records);
        } catch (Exception e) {
            Assert.fail("Failed to upload " + fileName, e);
        }
        FileUtils.deleteQuietly(new File(fileName));
    }

    protected void cleanupDynamicSource() {
        try {
            HdfsUtils.rmdir(configuration, INPUT_PATH + "/" + DYNAMIC_SOURCE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete dynamic source dir.", e);
        }
    }

    protected Map<Object, Integer> histogram(List<GenericRecord> records, String column) {
        Map<Object, Integer> results = new HashMap<>();
        for (GenericRecord record : records) {
            Object value = record.get(column);
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
