package com.latticeengines.dataflow.functionalframework;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
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
    private Configuration configuration = new Configuration();

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
        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();
        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro"));

        FileSystem fs = FileSystem.get(configuration);
        doCopy(fs, entries);
    }

    protected Map<String, Table> getSources() {
        Map<String, Table> sources = new HashMap<>();

        Map<String, String> sourcePaths = getSourcePaths();
        for (String key : sourcePaths.keySet()) {
            String path = sourcePaths.get(key);
            String idColumn = null;
            String lastModifiedColumn = null;
            sources.put(key,
                    MetadataConverter.readMetadataFromAvroFile(configuration, path, idColumn, lastModifiedColumn));
        }
        return sources;
    }

    protected Map<String, String> getSourcePaths() {
        Map<String, String> sources = new HashMap<>();
        sources.put("Lead", "/tmp/avro/Lead.avro");
        sources.put("Opportunity", "/tmp/avro/Opportunity.avro");
        sources.put("Contact", "/tmp/avro/Contact.avro");
        return sources;
    }

    protected List<GenericRecord> readOutput() {
        return AvroUtils.getDataFromGlob(configuration, TARGET_PATH + "/*.avro");
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

    protected void execute(DataFlowBuilder builder) {
        builder.setLocal(LOCAL);

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCETABLES", getSources());
        ctx.setProperty("CUSTOMER", "Customer");
        ctx.setProperty("TARGETPATH", TARGET_PATH);
        ctx.setProperty("TARGETTABLENAME", "Output");
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("FLOWNAME", "Flow");
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", configuration);
        ctx.setProperty("ENGINE", "TEZ");
        dataTransformationService.executeNamedTransformation(ctx, builder);
    }
}
