package com.latticeengines.pls.service.impl.dcp;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.pls.service.dcp.EnrichmentTemplateService;
import com.latticeengines.proxy.exposed.dcp.EnrichmentTemplateProxy;

@Service("enrichmentTemplateServiceImpl")
public class EnrichmentTemplateServiceImpl implements EnrichmentTemplateService {

    @Inject
    private EnrichmentTemplateProxy enrichmentTemplateProxy;

    @Override
    public EnrichmentTemplate createEnrichmentTemplate(String customerSpace, String layoutId, String templateName) {
        return enrichmentTemplateProxy.createEnrichmentTemplate(MultiTenantContext.getShortTenantId(), layoutId,
                templateName);
    }
}
