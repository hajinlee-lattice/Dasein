package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SalesforceRouteConfig extends SpringRouteBuilder {
    
    @SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(SalesforceRouteConfig.class);
    
    public SalesforceRouteConfig() {
    }
    
    @Override
    public void configure() throws Exception {
        from("direct:createJob"). //
        to("salesforce:createJob");

        from("direct:createBatchQuery"). //
        process(new SetupPropertiesForBatchQueryProcessor()). //
        to("salesforce:createBatchQuery"). //
        to("seda:batchInfo?size=10").transform(constant("OK"));
        
        from("direct:getBatch"). //
        to("salesforce:getBatch");

        from("direct:processResults"). //
        to("salesforce:getQueryResultIds"). //
        split(body()). //
        process(new SetupForQueryResultsProcessor()). //
        to("salesforce:getQueryResult"). //
        process(new XmlHandlerProcessor(getContext())). //
        recipientList(header("staxUri")). //
        end(). //
        process(new CloseJobProcessor());

        from("direct:getDescription"). //
        to("salesforce:getDescription");

        from("seda:batchInfo").process(new BatchInfoProcessor());
    }
 
}
