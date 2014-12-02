package com.latticeengines.eai.routes.marketo;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LoopConditionProcessor implements Processor {
    private static final Log log = LogFactory.getLog(LoopConditionProcessor.class);

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        Map<String, Object> body = exchange.getIn().getBody(Map.class);
        String nextPageToken = (String) body.get("nextPageToken");
        log.info("Next page token = " + nextPageToken);
        Boolean hasMoreResults = (Boolean) body.get("moreResult");
        exchange.setProperty(MarketoImportProperty.NEXTPAGETOKEN, nextPageToken);
        
        if (hasMoreResults) {
            log.info("Has more results...");
        } else {
            log.info("No more results...");
            exchange.setProperty("loop", null);
        }
        
        exchange.getOut().setBody(null);
    }
}
