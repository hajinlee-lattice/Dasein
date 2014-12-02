package com.latticeengines.eai.routes.strategy.marketo;

import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;

@Component("activityTypeImportStrategy")
public class ActivityTypeImportStrategy extends MarketoImportStrategyBase {
    
    public ActivityTypeImportStrategy() {
        super("Marketo.ActivityType");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        Map<String, Object> headers = getHeaders(ctx);
        Map<String, Object> activityTypes = template.requestBodyAndHeaders("direct:getActivityTypes", null, headers, Map.class);
        System.out.println(activityTypes);
    }

}
