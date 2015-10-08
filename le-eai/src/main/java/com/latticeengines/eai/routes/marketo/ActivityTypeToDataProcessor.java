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

public class ActivityTypeToDataProcessor implements Processor {

    private SpringCamelContext context;

    public ActivityTypeToDataProcessor(CamelContext context) {
        this.context = (SpringCamelContext) context;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void process(Exchange exchange) throws Exception {
        Table table = exchange.getProperty(MarketoImportProperty.TABLE, Table.class);

        if (table == null) {
            throw new RuntimeException("Table to be imported not available.");
        }

        Map<String, Object> body = exchange.getIn().getBody(Map.class);
        List<Map<String, Object>> activityList = (List) body.get("result");
        DataContainer dataContainer = new DataContainer(context, table);

        Map<String, Attribute> attrMap = table.getNameAttributeMap();
        for (Map<String, Object> entry : activityList) {
            dataContainer.newRecord();

            for (Map.Entry<String, Object> mapEntry : entry.entrySet()) {
                String attrName = mapEntry.getKey();
                attrName = attrName.replace(" ", "_");
                if (attrMap.containsKey(attrName)) {
                    dataContainer.setValueForAttribute(attrMap.get(attrName), mapEntry.getValue());
                }
            }
            dataContainer.endRecord();
        }
        exchange.setProperty(MarketoImportProperty.DATACONTAINER, dataContainer);
    }

}
