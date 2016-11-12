package com.latticeengines.dataflow.functionalframework;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class DataFlowOperationFunctionalTestNGBase extends DataFlowFunctionalTestNGBase {

    public final String TARGET_PATH = "/tmp/DataFlowOperationTestOutput";
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
        HdfsUtils.rmdir(configuration, "/tmp/avro");

        String lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro")
                .getPath();
        String opportunity = ClassLoader.getSystemResource(
                "com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        String contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro")
                .getPath();
        String feature = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Feature.avro")
                .getPath();
        String lead2 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead2.avro")
                .getPath();
        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();
        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + feature, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead2, "/tmp/avro"));

        FileSystem fs = FileSystem.get(configuration);
        doCopy(fs, entries);

        Logger.getLogger("com.latticeengines.domain.exposed.util.AttributeUtils").setLevel(Level.WARN);
    }

    protected Map<String, Table> getSources() {
        Map<String, Table> sources = new HashMap<>();

        Map<String, String> sourcePaths = getSourcePaths();
        for (String key : sourcePaths.keySet()) {
            String path = sourcePaths.get(key);
            String idColumn = null;
            String lastModifiedColumn = null;
            sources.put(key, MetadataConverter.getTable(configuration, path, idColumn, lastModifiedColumn));
        }
        return sources;
    }

    protected Map<String, String> getSourcePaths() {
        Map<String, String> sources = new HashMap<>();
        sources.put("Lead", "/tmp/avro/Lead.avro");
        sources.put("Opportunity", "/tmp/avro/Opportunity.avro");
        sources.put("Contact", "/tmp/avro/Contact.avro");
        sources.put("Feature", "/tmp/avro/Feature.avro");
        sources.put("Lead2", "/tmp/avro/Lead2.avro");
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

    protected Table execute(DataFlowBuilder builder, String tagetPath) {
        builder.setLocal(LOCAL);

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.SOURCETABLES, getSources());
        ctx.setProperty(DataFlowProperty.CUSTOMER, "Customer");
        ctx.setProperty(DataFlowProperty.TARGETPATH, tagetPath);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "Output");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "Flow");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, false);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, configuration);
        ctx.setProperty(DataFlowProperty.ENGINE, "FLINK");
        ctx.setProperty(DataFlowProperty.CASCADEMETADATA, true);
        return dataTransformationService.executeNamedTransformation(ctx, builder);
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
