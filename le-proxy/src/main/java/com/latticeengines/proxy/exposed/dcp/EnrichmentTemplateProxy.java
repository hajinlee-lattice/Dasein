package com.latticeengines.proxy.exposed.dcp;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("enrichmentTemplateProxy")
public class EnrichmentTemplateProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected EnrichmentTemplateProxy() {
        super("dcp");
    }

    private static final String PREFIX = "/customerspaces/{customerSpace}/enrichmenttemplate";

    public ResponseDocument<String> createEnrichmentTemplate(String customerSpace, String layoutId, String templateName) {
        String baseUrl = PREFIX + "/{layoutId}/{templateName}";
        String url = constructUrl(baseUrl, customerSpace, layoutId, templateName);
        return post("Create an EnrichmentTemplate from Layout", url, null, ResponseDocument.class);
    }
}
