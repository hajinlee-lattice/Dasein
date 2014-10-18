package com.latticeengines.dataflow.flowimpl.salesforce;

import java.util.HashMap;
import java.util.Map;

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
    public void setup() {
        createInitialEventTable.setLocal(true);
        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro").getPath();
        opportunityContactRole = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/OpportunityContactRole.avro").getPath();
    }
    
    @Test(groups = "functional")
    public void constructFlowDefinition() {
        Map<String, String> sources = new HashMap<>();
        sources.put("Lead", lead);
        sources.put("Opportunity", opportunity);
        sources.put("Contact", contact);
        sources.put("OpportunityContactRole", opportunityContactRole);
        
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        ctx.setProperty("TARGETPATH", "/tmp/EventTable");
        ctx.setProperty("QUEUE", "Priority0.MapReduce.0");
        ctx.setProperty("FLOWNAME", "CreateInitialEventTable");
        dataTransformationService.executeNamedTransformation(ctx, "createInitialEventTable");
        
    }
}
