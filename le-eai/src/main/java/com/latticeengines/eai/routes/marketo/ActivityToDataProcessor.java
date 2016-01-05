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

public class ActivityToDataProcessor implements Processor {

    private SpringCamelContext context;

    public ActivityToDataProcessor(CamelContext context) {
        this.context = (SpringCamelContext) context;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        List<?> activityList = exchange.getProperty(MarketoImportProperty.ACTIVITYRESULTLIST,
                List.class);
        Table table = exchange.getProperty(MarketoImportProperty.TABLE, Table.class);

        if (table == null) {
            throw new RuntimeException("Table to be imported not available.");
        }

        DataContainer dataContainer = exchange.getProperty(MarketoImportProperty.DATACONTAINER, DataContainer.class);

        if (dataContainer == null) {
            dataContainer = new DataContainer(context, table);
            exchange.setProperty(MarketoImportProperty.DATACONTAINER, dataContainer);
        }
        Map<String, Attribute> attrMap = table.getNameAttributeMap();
        for (Object entry : activityList) {
            dataContainer.newRecord();

            for (Map.Entry<String, Object> mapEntry : ((Map<String, Object>)entry).entrySet()) {
                String attrName = mapEntry.getKey();
                attrName = attrName.replace(" ", "_");
                if (attrMap.containsKey(attrName)) {
                    dataContainer.setValueForAttribute(attrMap.get(attrName), mapEntry.getValue());
                }
            }
            dataContainer.endRecord();
        }
    }
}
