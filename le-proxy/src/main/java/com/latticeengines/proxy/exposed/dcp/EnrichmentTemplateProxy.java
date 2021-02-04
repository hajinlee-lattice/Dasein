package com.latticeengines.proxy.exposed.dcp;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.CreateEnrichmentTemplateRequest;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.domain.exposed.dcp.ListEnrichmentTemplateRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("enrichmentTemplateProxy")
public class EnrichmentTemplateProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentTemplateProxy.class);

    protected EnrichmentTemplateProxy() {
        super("dcp");
    }

    private static final String PREFIX = "/customerspaces/{customerSpace}/enrichmenttemplate";

    public ResponseDocument<String> createEnrichmentTemplate(String customerSpace, String layoutId,
            String templateName) {
        String baseUrl = PREFIX + "/layout";
        String url = constructUrl(baseUrl, customerSpace);
        CreateEnrichmentTemplateRequest request = new CreateEnrichmentTemplateRequest(layoutId, templateName);
        return post("Create an EnrichmentTemplate from Layout", url, request, ResponseDocument.class);
    }

    public ResponseDocument<String> createEnrichmentTemplate(String customerSpace,
            EnrichmentTemplate enrichmentTemplate) {
        String baseUrl = PREFIX + "/create-template";
        String url = constructUrl(baseUrl, customerSpace);
        return post("Create an Enrichment Template", url, enrichmentTemplate, ResponseDocument.class);
    }

    public List<EnrichmentTemplateSummary> getEnrichmentTemplates(String customerSpace, String domain,
            String recordType, Boolean includeArchived, String createdBy) {
        String baseUrl = PREFIX + "/list";
        String url = constructUrl(baseUrl, customerSpace);
        ListEnrichmentTemplateRequest request = new ListEnrichmentTemplateRequest(customerSpace, domain, recordType,
                includeArchived, createdBy);

        List<?> rawList = post("List Enrichment Templates", url, request, List.class);
        return JsonUtils.convertList(rawList, EnrichmentTemplateSummary.class);
    }
}
