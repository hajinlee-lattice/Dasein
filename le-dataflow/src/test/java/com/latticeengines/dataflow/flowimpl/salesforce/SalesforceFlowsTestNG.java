package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class SalesforceFlowsTestNG extends DataFlowFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SalesforceFlowsTestNG.class);

    @Autowired
    private CreateInitialEventTable createInitialEventTable;

    @Autowired
    private DataTransformationService dataTransformationService;

    private String lead;
    private String opportunity;
    private String contact;
    private String opportunityContactRole;
    private Configuration config = new Configuration();

    private static final String PATH_TEST = String.format("/tmp/%s", SalesforceFlowsTestNG.class.getSimpleName());
    private static final String PATH_PDTABLE = String.format("%s/PDTable", PATH_TEST);
    private static final String PATH_EVENTTABLE = String.format("%s/EventTable", PATH_TEST);
    private static final String PATH_TMPEVENTTABLE = String.format("%s/TmpEventTable", PATH_TEST);
    private static final String PATH_AVRO = String.format("%s/avro", PATH_TEST);

    @BeforeMethod(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(config, PATH_TEST);
        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro")
                .getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro")
                .getPath();
        opportunityContactRole = ClassLoader
                .getSystemResource("com/latticeengines/dataflow/exposed/service/impl/OpportunityContactRole.avro")
                .getPath();

        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

        entries.add(new AbstractMap.SimpleEntry<>("file://" + lead, PATH_AVRO));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, PATH_AVRO));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, PATH_AVRO));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunityContactRole, PATH_AVRO));

        FileSystem fs = FileSystem.get(config);
        doCopy(fs, entries);

        log.info(
                "Lead.avro, Opportunity.avro, Contact.avro and OpportunityContactRole.avro have been copied to target hdfs dir");
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
            sources.put("Lead", PATH_AVRO + "/Lead.avro");
            sources.put("Opportunity", PATH_AVRO + "/Opportunity.avro");
            sources.put("Contact", PATH_AVRO + "/Contact.avro");
            sources.put("OpportunityContactRole", PATH_AVRO + "/OpportunityContactRole.avro");
        }

        // Execute the first flow
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty(DataFlowProperty.CUSTOMER, "customer1");
        ctx.setProperty(DataFlowProperty.SOURCES, sources);
        ctx.setProperty(DataFlowProperty.TARGETPATH, PATH_TMPEVENTTABLE);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "TmpEventTable");
        ctx.setProperty(DataFlowProperty.QUEUE, LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty(DataFlowProperty.FLOWNAME, "CreateInitialEventTable");
        ctx.setProperty(DataFlowProperty.CHECKPOINT, checkpoint);
        ctx.setProperty(DataFlowProperty.HADOOPCONF, config);
        ctx.setProperty(DataFlowProperty.ENGINE, "TEZ");
        dataTransformationService.executeNamedTransformation(ctx, "createInitialEventTable");
        verifyNumRows(config, PATH_TMPEVENTTABLE, 10787);

        // Execute the second flow, with the output of the first flow as input
        // into the second
        sources.put("EventTable", PATH_TMPEVENTTABLE + "/*.avro");
        ctx.setProperty(DataFlowProperty.TARGETPATH, PATH_PDTABLE);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "PDTable");
        ctx.setProperty(DataFlowProperty.FLOWNAME, "CreatePropDataInput");
        dataTransformationService.executeNamedTransformation(ctx, "createPropDataInput");
        verifyNumRows(config, PATH_PDTABLE, 106);

        // Execute the third flow, with the output of the first flow as input
        // into the third
        sources.put("EventTable", PATH_TMPEVENTTABLE + "/*.avro");
        ctx.setProperty(DataFlowProperty.TARGETPATH, PATH_EVENTTABLE);
        ctx.setProperty(DataFlowProperty.TARGETTABLENAME, "EventTable");
        ctx.setProperty(DataFlowProperty.FLOWNAME, "CreateFinalEventTable");

        ctx.setProperty(DataFlowProperty.EVENTDEFNEXPR,
                "StageName.equals(\"Contracting\") || StageName.equals(\"Closed Won\")");
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
