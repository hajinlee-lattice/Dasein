package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
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
    private DataTransformationService dataTransformationService;

    private String lead;
    private String opportunity;
    private String contact;
    private String opportunityContactRole;
    private Configuration config = new Configuration();

    @BeforeMethod(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(config, "/tmp/PDTable");
        HdfsUtils.rmdir(config, "/tmp/EventTable");
        HdfsUtils.rmdir(config, "/tmp/TmpEventTable");
        HdfsUtils.rmdir(config, "/tmp/checkpoints");
        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader
                .getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro")
                .getPath();
        opportunityContactRole = ClassLoader.getSystemResource(
                "com/latticeengines/dataflow/exposed/service/impl/OpportunityContactRole.avro").getPath();

        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunityContactRole, "/tmp/avro"));

        FileSystem fs = FileSystem.get(config);
        doCopy(fs, entries);
    }

    @Test(groups = "functional", dataProvider = "checkpointProvider")
    public void constructFlowDefinition(boolean checkpoint) throws Exception {
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
        ctx.setProperty(DataFlowProperty.CUSTOMER, "customer1");
        ctx.setProperty(DataFlowProperty.SOURCES, sources);
        ctx.setProperty(DataFlowProperty.TARGETPATH, "/tmp/TmpEventTable");
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "TmpEventTable");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "CreateInitialEventTable");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, checkpoint);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, config);
        ctx.setProperty(DataFlowProperty.ENGINE, "TEZ");
        dataTransformationService.executeNamedTransformation(ctx, "createInitialEventTable");
        verifyNumRows(config, "/tmp/TmpEventTable", 10787);

        // Execute the second flow, with the output of the first flow as input
        // into the second
        sources.put("EventTable", "/tmp/TmpEventTable/*.avro");
        ctx.setProperty(DataFlowProperty.TARGETPATH, "/tmp/PDTable");
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "PDTable");
        ctx.setProperty(DataFlowProperty.FLOWNAME, "CreatePropDataInput");
        dataTransformationService.executeNamedTransformation(ctx, "createPropDataInput");
        verifyNumRows(config, "/tmp/PDTable", 106);

        // Execute the third flow, with the output of the first flow as input
        // into the third
        sources.put("EventTable", "/tmp/TmpEventTable/*.avro");
        ctx.setProperty(DataFlowProperty.TARGETPATH, "/tmp/EventTable");
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "EventTable");
        ctx.setProperty(DataFlowProperty.FLOWNAME, "CreateFinalEventTable");

        ctx.setProperty(DataFlowProperty.EVENTDEFNEXPR, "StageName.equals(\"Contracting\") || StageName.equals(\"Closed Won\")");
        ctx.setProperty(DataFlowProperty.EVENTDEFNCOLS, new String[] { "StageName" });
        ctx.setProperty(DataFlowProperty.APPLYMETADATAPRUNING, true);

        dataTransformationService.executeNamedTransformation(ctx, "createFinalEventTable");
    }

    @DataProvider(name = "checkpointProvider")
    public Object[][] getEngine() {
        return new Object[][] { { true }, //
                { false } };
    }

}
