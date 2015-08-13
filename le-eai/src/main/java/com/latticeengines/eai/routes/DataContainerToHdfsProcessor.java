package com.latticeengines.eai.routes;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;

public class DataContainerToHdfsProcessor implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        Table table = exchange.getProperty(MarketoImportProperty.TABLE, Table.class);
        DataContainer dataContainer = exchange.getProperty(MarketoImportProperty.DATACONTAINER, DataContainer.class);
        dataContainer.endContainer();
        InputStream dataInputStream = new FileInputStream(dataContainer.getLocalDataFile());
        exchange.getIn().setHeader("hdfsUri",
                new HdfsUriGenerator().getHdfsUri(exchange, table, dataContainer.getLocalDataFile().getName()));
        exchange.getIn().setBody(dataInputStream);
    }

}
