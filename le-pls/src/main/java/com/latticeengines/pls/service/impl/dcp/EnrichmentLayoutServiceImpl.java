package com.latticeengines.pls.service.impl.dcp;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;
import com.latticeengines.pls.service.dcp.EnrichmentLayoutService;
import com.latticeengines.proxy.exposed.dcp.EnrichmentLayoutProxy;

@Service("enrichmentLayoutServiceImpl")
public class EnrichmentLayoutServiceImpl implements EnrichmentLayoutService {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentLayoutServiceImpl.class);

    @Inject
    EnrichmentLayoutProxy enrichmentLayoutProxy;

    @Override
    public EnrichmentLayoutOperationResult create(String customerId, EnrichmentLayout enrichmentLayout) {
        EnrichmentLayoutOperationResult result;
        try {
            result = enrichmentLayoutProxy.create(customerId, enrichmentLayout);
        } catch (Exception exception) {
            String msg = "Exception while trying to create enrichment layout.  " + enrichmentLayout.toString() + "\n";
            log.error(msg, exception);
            result = new EnrichmentLayoutOperationResult(false,
                    "Error while trying to save enrichment layout.  Please try again.");
        }
        return result;
    }

    @Override
    public EnrichmentLayoutDetail getEnrichmentLayoutBySourceId(String customerId, String sourceId) {
        EnrichmentLayoutDetail enrichmentLayoutDetail;
        try {
            enrichmentLayoutDetail = enrichmentLayoutProxy.getEnrichmentLayoutBySourceId(customerId, sourceId);
        }
        catch (Exception exception) {
            String msg = String.format("Exception while trying to retrieve enrichment layout sourceId = %s", sourceId);
            log.error(msg, exception);
            enrichmentLayoutDetail = null;
        }
        return enrichmentLayoutDetail;
    }

    @Override
    public EnrichmentLayoutDetail getEnrichmentLayoutByLayoutId(String customerId, String layoutId) {
        EnrichmentLayoutDetail enrichmentLayoutDetail;
        try {
            enrichmentLayoutDetail = enrichmentLayoutProxy.getEnrichmentLayoutByLayoutId(customerId, layoutId);
        }
        catch (Exception exception) {
            String msg = String.format("Exception while trying to retrieve enrichment layout layoutId = %s", layoutId);
            log.error(msg, exception);
            enrichmentLayoutDetail = null;
        }
        return enrichmentLayoutDetail;
    }

    @Override
    public EnrichmentLayoutOperationResult update(String customerId, EnrichmentLayout enrichmentLayout) {
        EnrichmentLayoutOperationResult result;
        try {
            result = enrichmentLayoutProxy.update(customerId, enrichmentLayout);
        }
        catch (Exception exception) {
            String msg = String.format("Exception while trying to update enrichment layout with layoutId = %s", enrichmentLayout.getLayoutId());
            log.error(msg, exception);
            result = new EnrichmentLayoutOperationResult(false, "Error while trying to update enrichment layout.");
        }
        return result;
    }

    @Override
    public EnrichmentLayoutOperationResult delete(String customerId, String layoutId) {

        return enrichmentLayoutProxy.deleteLayout(customerId, layoutId);
    }
}
