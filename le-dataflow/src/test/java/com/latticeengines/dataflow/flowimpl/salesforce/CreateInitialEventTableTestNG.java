package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.service.impl.DataTransformationServiceImpl;
import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public class CreateInitialEventTableTestNG extends DataFlowFunctionalTestNGBase {
    
    @Autowired
    private CreateInitialEventTable createInitialEventTable;
    
    @Autowired
    private DataTransformationServiceImpl dataTransformationService;
    
    private String lead;
    private String opportunity;
    private String contact;
    private String opportunityContactRole;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        createInitialEventTable.setLocal(true);
        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro").getPath();
        opportunityContactRole = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/OpportunityContactRole.avro").getPath();
        
        List<Pair<String, String>> entries = new ArrayList<>();
        
        entries.add(new Pair<>("file://" + lead, "/tmp/avro"));
        entries.add(new Pair<>("file://" + opportunity, "/tmp/avro"));
        entries.add(new Pair<>("file://" + contact, "/tmp/avro"));
        entries.add(new Pair<>("file://" + opportunityContactRole, "/tmp/avro"));
        
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
        
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("TARGETPATH", "/tmp/EventTable");
        ctx.setProperty("QUEUE", "Priority0.MapReduce.0");
        ctx.setProperty("FLOWNAME", "CreateInitialEventTable");
        dataTransformationService.executeNamedTransformation(ctx, "createInitialEventTable");
        
    }
}
