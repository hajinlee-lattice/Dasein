package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchInfo;

public class BatchInfoProcessor implements Processor {
    
    @Override
    public void process(Exchange exchange) throws Exception {
        Thread.sleep(2000L);
        BatchInfo batchInfo = exchange.getIn().getBody(BatchInfo.class);
        ProducerTemplate producer = exchange.getContext().createProducerTemplate();
        
        switch (batchInfo.getState()) {
        
        case IN_PROGRESS:
        case QUEUED:
            batchInfo = producer.requestBody("direct:getBatch", batchInfo, BatchInfo.class);
            exchange.getIn().setBody(batchInfo);
            producer.send("seda:batchInfo", exchange);
            break;
        case NOT_PROCESSED:
            break;
        case FAILED:
            break;
        case COMPLETED:
            exchange.setProperty(SalesforceImportProperty.BATCHINFO, batchInfo);
            producer.send("direct:processResults", exchange);
            break;
        default:
            break;
        }
    }
    
}
