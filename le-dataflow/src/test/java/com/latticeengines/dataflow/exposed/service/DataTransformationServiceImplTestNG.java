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
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class DataTransformationServiceImplTestNG extends DataFlowFunctionalTestNGBase {

    @Autowired
    private DataTransformationService dataTransformationService;
    
    @Autowired
    private CascadingDataFlowBuilder sampleDataFlowBuilder;

    private Configuration config = new Configuration();
    private String lead;
    private String opportunity;
    private String contact;

    @BeforeMethod(groups = "functional")
    public void setup() throws Exception {
        sampleDataFlowBuilder.reset();
        if (sampleDataFlowBuilder.isLocal()) {
            config.set("fs.defaultFS", "file:///");
        }
        
        HdfsUtils.rmdir(config, "/tmp/EventTable");
        HdfsUtils.rmdir(config, "/tmp/checkpoints");
        HdfsUtils.rmdir(config, "/tmp/avro");

        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro").getPath();

        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro"));
        
        if (!sampleDataFlowBuilder.isLocal()) {
            lead = "/tmp/avro/Lead.avro";
            opportunity = "/tmp/avro/Opportunity.avro";
            FileSystem fs = FileSystem.get(config);
            doCopy(fs, entries);
        } 
    }

    @Test(groups = "functional", dataProvider = "engineProvider")
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
        ctx.setProperty("ENGINE", "TEZ");
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
}
