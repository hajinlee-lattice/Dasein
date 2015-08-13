package com.latticeengines.eai.routes;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;

public class AvroHdfsProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        Table table = exchange.getProperty(MarketoImportProperty.TABLE, Table.class);
        DataContainer avroContainer = exchange.getProperty(MarketoImportProperty.AVROCONTAINER, DataContainer.class);
        avroContainer.endContainer();
        InputStream avroInputStream = new FileInputStream(avroContainer.getLocalAvroFile());
        exchange.getIn().setHeader("hdfsUri",
                new HdfsUriGenerator().getHdfsUri(exchange, table, avroContainer.getLocalAvroFile().getName()));
        exchange.getIn().setBody(avroInputStream);
    }

}
