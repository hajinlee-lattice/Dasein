package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.spring.SpringRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("salesforceRouteConfig")
public class SalesforceRouteConfig extends SpringRouteBuilder {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SalesforceRouteConfig.class);

    @Value("${eai.max.redeliveries}")
    private int maximumRedeliveries;

    @Value("${eai.backoff.multiplier}")
    private int backoffMultiplier;

    public SalesforceRouteConfig() {
    }

    @Override
    public void configure() throws Exception {

        errorHandler(defaultErrorHandler(). //
                maximumRedeliveries(maximumRedeliveries). //
                backOffMultiplier(backoffMultiplier));

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
                recipientList(header("hdfsUri")). //
                end().//
                process(new SetLastModifiedDateProcessor(getContext()));

        from("direct:getDescription"). //
                to("salesforce:getDescription");

        from("seda:batchInfo?concurrentConsumers=4").process(new BatchInfoProcessor());

    }

}
