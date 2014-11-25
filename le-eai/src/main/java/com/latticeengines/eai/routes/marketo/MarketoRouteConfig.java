package com.latticeengines.eai.routes.marketo;

import org.apache.camel.Processor;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.spring.SpringRouteBuilder;

public class MarketoRouteConfig extends SpringRouteBuilder {

    @Override
    public void configure() throws Exception {
        Processor baseUrlProcessor = new GenerateBaseUrlProcessor();
        JacksonDataFormat dataFormat = new JacksonDataFormat();
        
        from("direct:getToken"). //
        process(baseUrlProcessor). //
        process(new GenerateAccessTokenUrlProcessor()). //
        recipientList(header("tokenUrl")). //
        unmarshal(dataFormat);

        from("direct:getLeadActivities"). //
        process(baseUrlProcessor). //
        process(new GenerateLeadActivitiesUrlProcessor()). //
        recipientList(header("activitiesUrl"));

        from("direct:getActivityTypes"). //
        process(baseUrlProcessor). //
        process(new GenerateActivityTypesUrlProcessor()). //
        recipientList(header("activityTypesUrl")). //
        unmarshal(dataFormat);

        from("direct:getLeadMetadata"). //
        process(baseUrlProcessor). //
        process(new GenerateLeadMetadataUrlProcessor()). //
        recipientList(header("leadMetadataUrl")). //
        unmarshal(dataFormat);

        from("direct:getPagingToken"). //
        process(baseUrlProcessor). //
        process(new GeneratePagingTokenUrlProcessor()). //
        recipientList(header("pagingTokenUrl")). //
        unmarshal(dataFormat);
    }
}
