package com.latticeengines.eai.routes.salesforce;

import java.io.InputStream;
import java.util.Scanner;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import com.latticeengines.domain.exposed.eai.Table;

public class InputStreamToStringConverter implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        Table table = (Table) exchange.getProperty(SalesforceImportProperty.TABLE);
        String result = convertStreamToString(exchange.getIn().getBody(InputStream.class));
        System.out.println("Table = " + table.getName());
        System.out.println(result);
        exchange.getOut().setBody(result);
    }

    private static String convertStreamToString(InputStream is) {
        try (Scanner scanner = new Scanner(is)) {
            Scanner delimitedScanner = scanner.useDelimiter("\\A");
            return delimitedScanner.hasNext() ? scanner.next() : "";
        }
    }
    
}
