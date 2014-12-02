package com.latticeengines.eai.routes.marketo;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import com.latticeengines.domain.exposed.eai.ImportContext;

public class SetPropertiesFromImportContextProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        ImportContext ctx = exchange.getIn().getHeader(MarketoImportProperty.IMPORTCONTEXT, ImportContext.class);
        for (Map.Entry<String, Object> entry : ctx.getEntries()) {
            exchange.setProperty(entry.getKey(), entry.getValue());
        }
    }
}