package com.latticeengines.serviceflows.workflow.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;

public class InternalResourceRestApiProxy extends BaseRestApiProxy {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(InternalResourceRestApiProxy.class);

    private String internalResourceHostPort;

    public InternalResourceRestApiProxy(String internalResourceHostPort) {
        super();
        this.internalResourceHostPort = internalResourceHostPort;
    }

    @Override
    public String getRestApiHostPort() {
        return internalResourceHostPort;
    }

    public void createDefaultTargetMarket(String tenantId) {
        try {
            restTemplate.postForObject(constructUrl("pls/internal/targetmarkets/default", tenantId), null, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("createDefaultTargetMarket: Remote call failure", e);
        }
    }

    public TargetMarket findTargetMarketByName(String targetMarketName, String tenantId) {
        try {
            return restTemplate.getForObject(constructUrl("pls/internal/targetmarkets/", targetMarketName, tenantId),
                    TargetMarket.class);
        } catch (Exception e) {
            throw new RuntimeException("findTargetMarketByName: Remote call failure", e);
        }
    }

    public void updateTargetMarket(TargetMarket targetMarket, String tenantId) {
        try {
            restTemplate.put(constructUrl("pls/internal/targetmarkets/", targetMarket.getName(), tenantId),
                    targetMarket);
        } catch (Exception e) {
            throw new RuntimeException("updateTargetMarket: Remote call failure", e);
        }
    }

    public void deleteTargetMarketByName(String targetMarketName, String tenantId) {
        try {
            restTemplate.delete(constructUrl("pls/internal/targetmarkets/", targetMarketName, tenantId));
        } catch (Exception e) {
            throw new RuntimeException("deleteTargetMarketByName: Remote call failure", e);
        }
    }

    public ModelSummary getModelSummaryFromApplicationId(String applicationId, String tenantId) {
        try {
            return restTemplate.getForObject(constructUrl("pls/internal/modelsummaries", applicationId, tenantId),
                    ModelSummary.class);
        } catch (Exception e) {
            throw new RuntimeException("getModelSummaryFromApplicationId: Remote call failure", e);
        }
    }

    public void registerReport(String targetMarketName, Report report, String tenantId) {
        try {
            String url = constructUrl("pls/internal/targetmarkets", targetMarketName, "reports", tenantId);
            log.info(String.format("Posting to %s", url));
            restTemplate.postForObject(url, report, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("registerReport: Remote call failure", e);
        }
    }
}
