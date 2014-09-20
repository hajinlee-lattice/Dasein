package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SalesforceRouteConfig extends SpringRouteBuilder {
    
    private static final Log log = LogFactory.getLog(SalesforceRouteConfig.class);
    
    public SalesforceRouteConfig() {
    }
    
    @Override
    public void configure() throws Exception {
        
        PropertySetter propertySetter = new PropertySetter();
        BatchInfoProcessor batchInfoProcessor = new BatchInfoProcessor(getContext().createProducerTemplate());
        
        from("direct:createJob"). //
        to("salesforce:createJob");

        from("direct:createBatchQuery"). //
        process(propertySetter). //
        to("salesforce:createBatchQuery"). //
        to("seda:batchInfo?size=10").transform(constant("OK"));
        
        from("direct:getBatch"). //
        to("salesforce:getBatch");

        from("direct:getQueryResultIds"). //
        to("salesforce:getQueryResultIds");
        
        from("direct:getQueryResult"). //
        to("salesforce:getQueryResult");
        
        from("direct:getDescription"). //
        to("salesforce:getDescription");

        from("seda:batchInfo").process(batchInfoProcessor);
    }
 
}
