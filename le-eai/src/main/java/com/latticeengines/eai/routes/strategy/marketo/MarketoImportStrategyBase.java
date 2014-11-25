package com.latticeengines.eai.routes.strategy.marketo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.converter.AvroTypeConverter;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;
import com.latticeengines.eai.routes.strategy.ImportStrategy;

@Component
public abstract class MarketoImportStrategyBase extends ImportStrategy {
    
    @Autowired
    private AvroTypeConverter marketoToAvroTypeConverter;

    public MarketoImportStrategyBase(String name) {
        super(name);
    }
    
    protected Map<String, Object> getHeaders(ImportContext ctx) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(MarketoImportProperty.HOST, ctx.getProperty(MarketoImportProperty.HOST, String.class));
        headers.put(MarketoImportProperty.CLIENTID, ctx.getProperty(MarketoImportProperty.CLIENTID, String.class));
        headers.put(MarketoImportProperty.CLIENTSECRET,
                ctx.getProperty(MarketoImportProperty.CLIENTSECRET, String.class));
        headers.put(MarketoImportProperty.ACCESSTOKEN, ctx.getProperty(MarketoImportProperty.ACCESSTOKEN, String.class));
        headers.put(Exchange.CONTENT_TYPE, "application/json");
        return headers;
    }
    
    @Override
    protected AvroTypeConverter getAvroTypeConverter() {
        return marketoToAvroTypeConverter;
    }
    
    @Override
    public Table importMetadata(ProducerTemplate template, Table table, ImportContext ctx) {
        AvroTypeConverter converter = getAvroTypeConverter();
        List<Attribute> attributes = table.getAttributes();
        for (Attribute attribute : attributes) {
            attribute.setPhysicalDataType(converter.convertTypeToAvro(attribute.getLogicalDataType()).name());
        }
        return table;
    }
    
    public boolean needsPageToken() {
        return false;
    }
}
