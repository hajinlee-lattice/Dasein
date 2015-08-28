package com.latticeengines.dataflow.exposed.service;


import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class DataTransformationServiceImplTestNG extends DataFlowFunctionalTestNGBase {

    @Autowired
    private DataTransformationService dataTransformationService;
    
    @Autowired
    private CascadingDataFlowBuilder sampleDataFlowBuilder;

    @Autowired
    private CascadingDataFlowBuilder tableWithExtractsDataFlowBuilder;

    private Configuration config = new Configuration();
    private String lead;
    private String opportunity;
    private String contact;
    private String extract1;
    private String extract2;
    private String extract3;

    @BeforeMethod(groups = "functional")
    public void setup() throws Exception {
        sampleDataFlowBuilder.reset();
        tableWithExtractsDataFlowBuilder.reset();
        if (sampleDataFlowBuilder.isLocal()) {
            config.set("fs.defaultFS", "file:///");
        }
        
        HdfsUtils.rmdir(config, "/tmp/EventTable");
        HdfsUtils.rmdir(config, "/tmp/CombinedImportTable");
        HdfsUtils.rmdir(config, "/tmp/checkpoints");
        HdfsUtils.rmdir(config, "/tmp/avro");

        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro").getPath();
        extract1 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file1").getPath() + "/*.avro";
        extract2 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file2").getPath() + "/*.avro";
        extract3 = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/file3").getPath() + "/*.avro";

        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + extract1, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + extract2, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + extract3, "/tmp/avro"));
        
        if (!sampleDataFlowBuilder.isLocal()) {
            lead = "/tmp/avro/Lead.avro";
            opportunity = "/tmp/avro/Opportunity.avro";
            FileSystem fs = FileSystem.get(config);
            doCopy(fs, entries);
        } 

        if (!tableWithExtractsDataFlowBuilder.isLocal()) {
            extract1 = "/tmp/avro/file1.avro";
            extract2 = "/tmp/avro/file2.avro";
            extract3 = "/tmp/avro/file3.avro";
            FileSystem fs = FileSystem.get(config);
            doCopy(fs, entries);
        } 
    }

    @Test(groups = "functional", dataProvider = "engineProvider", enabled = true)
    public void executeNamedTransformation(String engine) throws Exception {
        Map<String, String> sources = new HashMap<>();
        sources.put("Lead", lead);
        sources.put("Opportunity", opportunity);

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", "customer1");
        ctx.setProperty("TARGETPATH", "/tmp/EventTable");
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("FLOWNAME", "SampleDataFlow-Lead*Oppty");
        ctx.setProperty("CHECKPOINT", true);
        ctx.setProperty("HADOOPCONF", config);
        ctx.setProperty("ENGINE", engine);
        dataTransformationService.executeNamedTransformation(ctx, "sampleDataFlowBuilder");
        verifyNumRows(config, "/tmp/EventTable", 308);
    }
    
    @DataProvider(name = "engineProvider")
    public Object[][] getEngine() {
        return new Object[][] {
                { "MR" }, //
                { "TEZ" }
        };
    }
    
    @Test(groups = "functional", dataProvider = "engineProvider")
    public void executeNamedTransformationForTableSource(String engine) throws Exception {
        Map<String, String> sources = new HashMap<>();
        Table table = new Table();
        table.setName("source");
        Extract e1 = new Extract();
        e1.setName("e1");
        e1.setPath(extract1);
        Extract e2 = new Extract();
        e2.setName("e2");
        e2.setPath(extract2);
        Extract e3 = new Extract();
        e3.setName("e3");
        e3.setPath(extract3);
        table.addExtract(e1);
        table.addExtract(e2);
        table.addExtract(e3);

        sources.put("Source", table.toString());

        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("CUSTOMER", "customer2");
        ctx.setProperty("TARGETPATH", "/tmp/CombinedImportTable");
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("FLOWNAME", "TableWithExtractsDataFlow");
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", config);
        ctx.setProperty("ENGINE", "MR");
        dataTransformationService.executeNamedTransformation(ctx, "tableWithExtractsDataFlowBuilder");
        verifyNumRows(config, "/tmp/CombinedImportTable", 7);
    }
    
}
