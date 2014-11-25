package com.latticeengines.eai.routes.strategy.marketo;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;

@Component("activityImportStrategy")
public class ActivityImportStrategy extends MarketoImportStrategyBase {
    private static final Log log = LogFactory.getLog(ActivityImportStrategy.class);

    public ActivityImportStrategy() {
        super("Marketo.Activity");
    }

    @Override
    public void importData(ProducerTemplate template, Table table, ImportContext ctx) {
    }

    @Override
    public Table importMetadata(ProducerTemplate template, Table table, ImportContext ctx) {
        return super.importMetadata(template, table, ctx);
    }
    
    @Override
    public boolean needsPageToken() {
        return true;
    }
    
}
