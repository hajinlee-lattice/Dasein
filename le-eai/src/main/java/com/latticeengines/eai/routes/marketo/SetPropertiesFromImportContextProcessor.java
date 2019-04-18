package com.latticeengines.eai.routes.marketo;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Table;

public class SetPropertiesFromImportContextProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(SetPropertiesFromImportContextProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        ImportContext ctx = exchange.getIn().getHeader(MarketoImportProperty.IMPORTCONTEXT, ImportContext.class);
        log.info("Import context properties:");

        if (ctx != null) {
            for (Map.Entry<String, Object> entry : ctx.getEntries()) {
                log.info("Property " + entry.getKey() + " = " + entry.getValue());
                exchange.setProperty(entry.getKey(), entry.getValue());
            }
            exchange.setProperty(MarketoImportProperty.IMPORTCONTEXT, ctx);
        } else {
            log.warn("ImportContext is null.");
        }

        Table table = exchange.getIn().getHeader(MarketoImportProperty.TABLE, Table.class);

        if (table != null) {
            exchange.setProperty(MarketoImportProperty.TABLE, table);
        }
    }
}
