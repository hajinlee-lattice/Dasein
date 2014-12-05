package com.latticeengines.eai.routes.marketo;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.eai.ImportContext;

public class SetPropertiesFromImportContextProcessor implements Processor {
    
    private final static Log log = LogFactory.getLog(SetPropertiesFromImportContextProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        ImportContext ctx = exchange.getIn().getHeader(MarketoImportProperty.IMPORTCONTEXT, ImportContext.class);
        log.info("Import context properties:");
        for (Map.Entry<String, Object> entry : ctx.getEntries()) {
            log.info("Property " + entry.getKey() + " = " + entry.getValue());
            exchange.setProperty(entry.getKey(), entry.getValue());
        }
    }
}