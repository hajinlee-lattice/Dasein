package com.latticeengines.eai.service.impl.marketo.strategy;

import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.marketo.MarketoImportProperty;

@Component("accessTokenImportStrategy")
public class AccessTokenImportStrategy extends MarketoImportStrategyBase {

    public AccessTokenImportStrategy() {
        super("Marketo.AccessToken");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        Map<String, Object> headers = getHeaders(ctx);
        headers.put(MarketoImportProperty.CLIENTID, ctx.getProperty(MarketoImportProperty.CLIENTID, String.class));
        headers.put(MarketoImportProperty.CLIENTSECRET,
                ctx.getProperty(MarketoImportProperty.CLIENTSECRET, String.class));
        Map<String, String> tokenMap = template.requestBodyAndHeaders("direct:getToken", null, headers, Map.class);
        ctx.setProperty(MarketoImportProperty.ACCESSTOKEN, tokenMap.get("access_token"));
    }

}
