package com.latticeengines.dataflow.exposed.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.functionalframework.DataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

public class DataTransformationServiceImplTestNG extends DataFlowFunctionalTestNGBase {

    @Autowired
    private DataTransformationServiceImpl dataTransformationService;
    
    private String lead;
    private String opportunity;
    private String contact;
    
    @BeforeClass(groups = "functional")
    public void setup() {
        lead = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Lead.avro").getPath();
        opportunity = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("com/latticeengines/dataflow/exposed/service/impl/Contact.avro").getPath();
    }

    @Test(groups = "functional")
    public void executeNamedTransformation() {
        Map<String, String> sources = new HashMap<>();
        sources.put("lead", lead);
        sources.put("oppty", opportunity);
        
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("SOURCES", sources);
        dataTransformationService.executeNamedTransformation(ctx, "sampleDataFlowBuilder");
    }
}
