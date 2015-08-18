package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.service.impl.DataTransformationServiceImpl;
import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class SalesforceFlowsTestNG extends DataFlowFunctionalTestNGBase {

    @Autowired
    private CreateFinalEventTable createFinalEventTable;

    @Autowired
    private CreateInitialEventTable createInitialEventTable;

    @Autowired
    private CreatePropDataInput createPropDataInput;

    @Autowired
    private DataTransformationServiceImpl dataTransformationService;

    private String lead;
    private String opportunity;
    private String contact;
    private String opportunityContactRole;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        //createFinalEventTable.setLocal(true);
        //createInitialEventTable.setLocal(true);
        //createPropDataInput.setLocal(true);
        Configuration config = new Configuration();
        HdfsUtils.rmdir(config, "/tmp/PDTable");
        HdfsUtils.rmdir(config, "/tmp/EventTable");
        HdfsUtils.rmdir(config, "/tmp/TmpEventTable");
        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro").getPath();
        opportunityContactRole = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/OpportunityContactRole.avro").getPath();

        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunityContactRole, "/tmp/avro"));

        FileSystem fs = FileSystem.get(new Configuration());
        doCopy(fs, entries);
    }

    @Test(groups = "functional")
    public void constructFlowDefinition() {
        Map<String, String> sources = new HashMap<>();

        if (createInitialEventTable.isLocal()) {
            sources.put("Lead", lead);
            sources.put("Opportunity", opportunity);
            sources.put("Contact", contact);
            sources.put("OpportunityContactRole", opportunityContactRole);
        } else {
            sources.put("Lead", "/tmp/avro/Lead.avro");
            sources.put("Opportunity", "/tmp/avro/Opportunity.avro");
            sources.put("Contact", "/tmp/avro/Contact.avro");
            sources.put("OpportunityContactRole", "/tmp/avro/OpportunityContactRole.avro");
        }

        // Execute the first flow
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("CUSTOMER", "customer1");
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("TARGETPATH", "/tmp/TmpEventTable");
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("FLOWNAME", "CreateInitialEventTable");
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", new Configuration());
        dataTransformationService.executeNamedTransformation(ctx, "createInitialEventTable");

        // Execute the second flow, with the output of the first flow as input into the second
        sources.put("EventTable", "/tmp/TmpEventTable/*.avro");
        ctx.setProperty("TARGETPATH", "/tmp/PDTable");
        ctx.setProperty("FLOWNAME", "CreatePropDataInput");
        dataTransformationService.executeNamedTransformation(ctx, "createPropDataInput");

        // Execute the third flow, with the output of the first flow as input into the third
        sources.put("EventTable", "/tmp/TmpEventTable/*.avro");
        ctx.setProperty("TARGETPATH", "/tmp/EventTable");
        ctx.setProperty("FLOWNAME", "CreateFinalEventTable");

        ctx.setProperty("EVENTDEFNEXPR", "StageName.equals(\"Contracting\") || StageName.equals(\"Closed Won\")");
        ctx.setProperty("EVENTDEFNCOLS", new String[] { "StageName" });
        ctx.setProperty("APPLYMETADATAPRUNING", true);

        dataTransformationService.executeNamedTransformation(ctx, "createFinalEventTable");
    }
}
