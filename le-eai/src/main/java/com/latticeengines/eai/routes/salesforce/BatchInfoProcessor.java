package com.latticeengines.eai.routes.salesforce;

import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import org.apache.camel.Body;
import org.apache.camel.Exchange;
import org.apache.camel.Header;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.salesforce.SalesforceEndpointConfig;
import org.apache.camel.component.salesforce.api.dto.bulk.BatchInfo;
import org.apache.camel.component.salesforce.api.dto.bulk.JobInfo;

import com.latticeengines.domain.exposed.eai.Table;

public class BatchInfoProcessor implements Processor {
    
    private ProducerTemplate producer;
    private ThreadLocal<Table> tableContext = new ThreadLocal<Table>();
    private ThreadLocal<JobInfo> jobInfoContext = new ThreadLocal<JobInfo>();
    
    public BatchInfoProcessor(ProducerTemplate producer) {
        this.producer = producer;
    }
    
    public void init(@Body JobInfo jobInfo, @Header(SalesforceImportHeader.TABLE) Table table) {
        tableContext.set(table);
        jobInfoContext.set(jobInfo);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Thread.sleep(2000L);
        BatchInfo batchInfo = exchange.getIn().getBody(BatchInfo.class);
        ProducerTemplate producer = exchange.getContext().createProducerTemplate();
        
        switch (batchInfo.getState()) {
        
        case IN_PROGRESS:
        case QUEUED:
            batchInfo = producer.requestBody("direct:getBatch", batchInfo, BatchInfo.class);
            producer.sendBody("seda:batchInfo", batchInfo);
            break;
        case NOT_PROCESSED:
            break;
        case FAILED:
            break;
        case COMPLETED:
            parseAndCreateAvroFile(batchInfo);
            break;
        default:
            break;
        }
    }
    
    private void parseAndCreateAvroFile(BatchInfo batchInfo) {
        @SuppressWarnings("unchecked")
        List<String> resultIds = producer.requestBody("direct:getQueryResultIds", batchInfo, List.class);
        
        try {
            for (String resultId : resultIds) {
                InputStream results = producer.requestBodyAndHeader("direct:getQueryResult", batchInfo,
                        SalesforceEndpointConfig.RESULT_ID, resultId, InputStream.class);
                String s = convertStreamToString(results);
                System.out.println(s);
            }
        } finally {
            producer.requestBody("salesforce:closeJob", jobInfoContext.get(), JobInfo.class);
        }
    }

    private static String convertStreamToString(InputStream is) {
        try (Scanner scanner = new Scanner(is)) {
            Scanner delimitedScanner = scanner.useDelimiter("\\A");
            return delimitedScanner.hasNext() ? scanner.next() : "";
        }
    }

}
