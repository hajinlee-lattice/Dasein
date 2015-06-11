package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Processor;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.spring.SpringRouteBuilder;
import org.springframework.beans.factory.annotation.Value;

public class MarketoRouteConfig extends SpringRouteBuilder {
    
    @Value("${eai.max.redeliveries}")
    private int maximumRedeliveries;

    @Value("${eai.backoff.multiplier}")
    private int backoffMultiplier;

    @Override
    public void configure() throws Exception {
        Processor baseUrlProcessor = new GenerateBaseUrlProcessor();
        Processor setPropertiesFromImportCtxProcessor = new SetPropertiesFromImportContextProcessor();
        JacksonDataFormat dataFormat = new JacksonDataFormat();

        errorHandler(defaultErrorHandler(). //
                maximumRedeliveries(maximumRedeliveries). //
                backOffMultiplier(backoffMultiplier));

        from("direct:getToken"). //
        process(setPropertiesFromImportCtxProcessor). //
        process(baseUrlProcessor). //
        process(new GenerateAccessTokenUrlProcessor()). //
        recipientList(header("tokenUrl")). //
        unmarshal(dataFormat);

        from("direct:getLeadActivities"). //
        process(baseUrlProcessor). //
        process(new GenerateLeadActivitiesUrlProcessor()). //
        recipientList(header("activitiesUrl")). //
        unmarshal(dataFormat). //
        process(new LoopConditionProcessor()). //
        to("seda:createActivities?size=10");
        
        from("direct:getAllLeadActivities"). //
        process(setPropertiesFromImportCtxProcessor). //
        setProperty("loop", constant("direct://getLeadActivities")). //
        dynamicRouter().property("loop");
        
        from("direct:getActivityTypes"). //
        process(setPropertiesFromImportCtxProcessor). //
        process(baseUrlProcessor). //
        process(new GenerateActivityTypesUrlProcessor()). //
        recipientList(header("activityTypesUrl")). //
        unmarshal(dataFormat);

        from("direct:getLeadMetadata"). //
        process(setPropertiesFromImportCtxProcessor). //
        process(baseUrlProcessor). //
        process(new GenerateLeadMetadataUrlProcessor()). //
        recipientList(header("leadMetadataUrl")). //
        unmarshal(dataFormat);

        from("direct:getPagingToken"). //
        process(setPropertiesFromImportCtxProcessor). //
        process(baseUrlProcessor). //
        process(new GeneratePagingTokenUrlProcessor()). //
        recipientList(header("pagingTokenUrl")). //
        unmarshal(dataFormat);

        from("direct:getLeads"). //
        process(setPropertiesFromImportCtxProcessor). //
        process(baseUrlProcessor). //
        process(new GenerateLeadsUrlProcessor()). //
        recipientList(header("leadsUrl"));
        
        from("seda:createActivities?concurrentConsumers=4"). //
        process(new ActivityToAvroProcessor());
        

    }
}
