package com.latticeengines.eai.routes;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import com.latticeengines.domain.exposed.eai.ImportProperty;

public class PropertySetter implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        exchange.setProperty(ImportProperty.TABLE, exchange.getIn().getHeader(ImportProperty.TABLE));
        exchange.setProperty(ImportProperty.IMPORTCTX, exchange.getIn().getHeader(ImportProperty.IMPORTCTX));
    }

}
