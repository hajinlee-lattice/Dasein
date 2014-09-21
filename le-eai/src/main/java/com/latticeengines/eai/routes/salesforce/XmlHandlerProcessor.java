package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spring.SpringCamelContext;

import com.latticeengines.domain.exposed.eai.Table;

public class XmlHandlerProcessor implements Processor {

    private ExtractDataXmlHandler extractDataXmlHandler;

    public XmlHandlerProcessor(CamelContext context) {
        extractDataXmlHandler = ((SpringCamelContext) context).getApplicationContext().getBean("extractDataXmlHandler",
                ExtractDataXmlHandler.class);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Table table = (Table) exchange.getProperty(SalesforceImportProperty.TABLE);
        extractDataXmlHandler.initialize(table);
    }

}
