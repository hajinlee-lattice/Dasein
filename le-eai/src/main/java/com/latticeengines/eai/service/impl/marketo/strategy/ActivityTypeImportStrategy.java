package com.latticeengines.eai.service.impl.marketo.strategy;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.util.AvroSchemaBuilder;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;

@Component("activityTypeImportStrategy")
public class ActivityTypeImportStrategy extends MarketoImportStrategyBase {
    
    public ActivityTypeImportStrategy() {
        super("Marketo.ActivityType");
    }

    @Override
    public Table importMetadata(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        table = super.importMetadata(template, table, filter, ctx);
        Schema schema = AvroSchemaBuilder.createSchema("ActivityType", table);
        table.setSchema(schema);
        return table;
    }
    
    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        ctx.setProperty(MarketoImportProperty.DOIMPORT, true);
        Map<String, Object> headers = getHeaders(ctx, table);
        template.sendBodyAndHeaders("direct:getActivityTypes", null, headers);
    }

}
