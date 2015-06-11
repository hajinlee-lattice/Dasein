package com.latticeengines.eai.routes.marketo;

import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class ActivityToAvroProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        List<?> activityList = exchange.getProperty(MarketoImportProperty.ACTIVITYRESULTLIST, List.class);
        System.out.println(activityList.get(0));
    }
}
