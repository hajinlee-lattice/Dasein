package com.latticeengines.eai.routes.marketo;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spring.SpringCamelContext;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.AvroContainer;
import com.latticeengines.eai.routes.HdfsUriGenerator;

public class ActivityToAvroProcessor implements Processor {

    private SpringCamelContext context;
    private static int numActivityBatches = 1;

    public ActivityToAvroProcessor(CamelContext context) {
        this.context = (SpringCamelContext) context;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        List<Map<String, Object>> activityList = exchange.getProperty(MarketoImportProperty.ACTIVITYRESULTLIST,
                List.class);
        Table table = exchange.getProperty(MarketoImportProperty.TABLE, Table.class);

        if (table == null) {
            throw new RuntimeException("Table to be imported not available.");
        }

        AvroContainer avroContainer = new AvroContainer(context, table, numActivityBatches++);
        Map<String, Attribute> attrMap = table.getNameAttributeMap();
        for (Map<String, Object> entry : activityList) {
            avroContainer.newRecord();

            for (Map.Entry<String, Object> mapEntry : entry.entrySet()) {
                String attrName = mapEntry.getKey();
                attrName = attrName.replace(" ", "_");
                if (attrMap.containsKey(attrName)) {
                    avroContainer.setValueForAttribute(attrMap.get(attrName), mapEntry.getValue());
                }
            }
            avroContainer.endRecord();
        }
        avroContainer.endContainer();
        InputStream avroInputStream = new FileInputStream(avroContainer.getLocalAvroFile());
        exchange.getIn().setHeader("hdfsUri",
                new HdfsUriGenerator().getHdfsUri(exchange, table, avroContainer.getLocalAvroFile().getName()));
        exchange.getIn().setBody(avroInputStream);
    }
}
