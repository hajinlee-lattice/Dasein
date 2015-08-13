package com.latticeengines.eai.service.impl.marketo.strategy;

import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;

@Component("pagingTokenImportStrategy")
public class PagingTokenImportStrategy extends MarketoImportStrategyBase {

    public PagingTokenImportStrategy() {
        super("Marketo.PagingToken");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        Map<String, Object> headers = getHeaders(ctx);
        Map<String, String> tokenMap = template.requestBodyAndHeaders("direct:getPagingToken", null, headers, Map.class);
        ctx.setProperty(MarketoImportProperty.NEXTPAGETOKEN, tokenMap.get("nextPageToken"));
    }

}
