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
        from("direct:createJob"). //
        to("salesforce:createJob");

        from("direct:createBatchQuery"). //
        to("salesforce:createBatchQuery?sObjectQuery=SELECT Name FROM Lead"). //
        to("seda:batchInfo?size=10").transform(constant("OK"));
        
        from("direct:getBatch"). //
        to("salesforce:getBatch");

        from("direct:getQueryResultIds"). //
        to("salesforce:getQueryResultIds");
        
        from("direct:getQueryResult"). //
        to("salesforce:getQueryResult");

        from("seda:batchInfo").process(new BatchInfoProcessor());
    }
 
}
