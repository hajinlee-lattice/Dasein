package com.latticeengines.pls.service.impl.dcp;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.pls.service.dcp.EnrichmentTemplateService;
import com.latticeengines.proxy.exposed.dcp.EnrichmentTemplateProxy;

@Service("enrichmentTemplateServiceImpl")
public class EnrichmentTemplateServiceImpl implements EnrichmentTemplateService {

    @Inject
    private EnrichmentTemplateProxy enrichmentTemplateProxy;

    @Override
    public ResponseDocument<String> createEnrichmentTemplate(String layoutId, String templateName) {
        return enrichmentTemplateProxy.createEnrichmentTemplate(MultiTenantContext.getShortTenantId(), layoutId,
                templateName);
    }

    @Override
    public ResponseDocument<String> createEnrichmentTemplate(EnrichmentTemplate enrichmentTemplate) {
        return enrichmentTemplateProxy.createEnrichmentTemplate(enrichmentTemplate);
    }
}
