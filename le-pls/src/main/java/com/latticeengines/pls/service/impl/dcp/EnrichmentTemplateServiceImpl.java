package com.latticeengines.pls.service.impl.dcp;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.CreateEnrichmentTemplateRequest;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.pls.service.dcp.EnrichmentTemplateService;
import com.latticeengines.proxy.exposed.dcp.EnrichmentTemplateProxy;

@Service("enrichmentTemplateServiceImpl")
public class EnrichmentTemplateServiceImpl implements EnrichmentTemplateService {

    @Inject
    private EnrichmentTemplateProxy enrichmentTemplateProxy;

    @Override
    public ResponseDocument<EnrichmentTemplateSummary> createEnrichmentTemplate(String layoutId, String templateName) {
        CreateEnrichmentTemplateRequest request = new CreateEnrichmentTemplateRequest(layoutId, templateName, MultiTenantContext.getEmailAddress());
        return enrichmentTemplateProxy.createEnrichmentTemplate(MultiTenantContext.getShortTenantId(), request);
    }

    @Override
    public ResponseDocument<EnrichmentTemplateSummary> createEnrichmentTemplate(EnrichmentTemplate enrichmentTemplate) {
        return enrichmentTemplateProxy.createEnrichmentTemplate(MultiTenantContext.getShortTenantId(),
                enrichmentTemplate);
    }

    @Override
    public List<EnrichmentTemplateSummary> getEnrichmentTemplates(String domain, String recordType,
            Boolean includeArchived, String createdBy) {
        return enrichmentTemplateProxy.getEnrichmentTemplates(MultiTenantContext.getShortTenantId(), domain, recordType,
                includeArchived, createdBy);
    }

    @Override
    public EnrichmentTemplateSummary getEnrichmentTemplate(String customerSpace, String templateId) {
        return enrichmentTemplateProxy.getEnrichmentTemplate(customerSpace, templateId);
    }
}
