package com.latticeengines.eai.routes.salesforce;


import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spring.SpringCamelContext;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;

public class SetLastModifiedDateProcessor implements Processor {

    private SpringCamelContext context;

    public SetLastModifiedDateProcessor(CamelContext context) {
        this.context = (SpringCamelContext) context;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        ImportContext importContext = exchange.getProperty(ImportProperty.IMPORTCTX, ImportContext.class);

        Table table = exchange.getProperty(ImportProperty.TABLE, Table.class);
        String beanName = "extractDataXmlHandlerFor" + table.getName();
        ExtractDataXmlHandler handler = context.getApplicationContext().getBean(beanName, ExtractDataXmlHandler.class);

        @SuppressWarnings("unchecked")
        Map<String, Long> processedRecordsMap = importContext.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);
        processedRecordsMap.put(table.getName(), handler.getProcessedRecords());
        @SuppressWarnings("unchecked")
        Map<String, Long> lastModifiedDateMap = importContext.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class);
        lastModifiedDateMap.put(table.getName(), handler.getLmkValue());
    }

}
