package com.latticeengines.eai.routes.marketo;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spring.SpringCamelContext;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.DataContainer;

public class ActivityToAvroProcessor implements Processor {

    private SpringCamelContext context;

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

        DataContainer avroContainer = exchange.getProperty(MarketoImportProperty.AVROCONTAINER, DataContainer.class);

        if (avroContainer == null) {
            avroContainer = new DataContainer(context, table);
            exchange.setProperty(MarketoImportProperty.AVROCONTAINER, avroContainer);
        }
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
    }
}
