package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.salesforce.api.dto.bulk.JobInfo;

public class CloseJobProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        JobInfo jobInfo = (JobInfo) exchange.getProperty(SalesforceImportProperty.JOBINFO);
        jobInfo  = exchange.getContext().createProducerTemplate().requestBody("salesforce:closeJob", jobInfo, JobInfo.class);
    }

}
