package com.latticeengines.eai.routes.marketo;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spring.SpringCamelContext;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.DataContainer;

public class ActivityTypeToAvroProcessor implements Processor {

    private SpringCamelContext context;

    public ActivityTypeToAvroProcessor(CamelContext context) {
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
        DataContainer avroContainer = new DataContainer(context, table);

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
        exchange.setProperty(MarketoImportProperty.AVROCONTAINER, avroContainer);
    }

}
