package com.latticeengines.eai.routes.salesforce;

import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchStateEnum;

public class BatchInfoProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        Thread.sleep(2000L);
        BatchInfo batchInfo = exchange.getIn().getBody(BatchInfo.class);
        ProducerTemplate producer = exchange.getContext().createProducerTemplate();
        
        if (batchInfo.getState() != BatchStateEnum.COMPLETED) {
            batchInfo = producer.requestBody("direct:getBatch", batchInfo, BatchInfo.class);
            producer.sendBody("seda:batchInfo", batchInfo);
            return;
        }
        
        @SuppressWarnings("unchecked")
        List<String> resultIds = producer.requestBody("direct:getQueryResultIds", batchInfo, List.class);
        
        for (String resultId : resultIds) {
            InputStream results = producer.requestBodyAndHeader("direct:getQueryResult", batchInfo,
                    SalesforceEndpointConfig.RESULT_ID, resultId, InputStream.class);
            String s = convertStreamToString(results);
            System.out.println(s);
        }
    }

    private static String convertStreamToString(InputStream is) {
        try (Scanner scanner = new Scanner(is)) {
            Scanner delimitedScanner = scanner.useDelimiter("\\A");
            return delimitedScanner.hasNext() ? scanner.next() : "";
        }
    }

}
