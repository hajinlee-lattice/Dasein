package com.latticeengines.serviceflows.workflow.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;

public class InternalResourceRestApiProxy extends BaseRestApiProxy {

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

    public TargetMarket createDefaultTargetMarket(String tenantId) {
        try {
            return restTemplate.postForObject(constructUrl("pls/internal/targetmarkets/default", tenantId), null,
                    TargetMarket.class);
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

    public void deleteAllTargetMarkets(String tenantId) {
        try {
            restTemplate.delete(constructUrl("pls/internal/targetmarkets/", tenantId));
        } catch (Exception e) {
            throw new RuntimeException("deleteAllTargetMarkets: Remote call failure", e);
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

    public void registerReport(Report report, String tenantId) {
        try {
            String url = constructUrl("pls/internal/reports", tenantId);
            log.info(String.format("Posting to %s", url));
            restTemplate.postForObject(url, report, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("registerReport: Remote call failure", e);
        }
    }

    public SourceFile findSourceFileByName(String name, String tenantId) {
        try {
            String url = constructUrl("pls/internal/sourcefiles", name, tenantId);
            log.info(String.format("Getting from %s", url));
            return restTemplate.getForObject(url, SourceFile.class);
        } catch (Exception e) {
            throw new RuntimeException("findSourceFileByName: Remote call failure", e);
        }
    }

    public void updateSourceFile(SourceFile sourceFile, String tenantId) {
        try {
            String url = constructUrl("pls/internal/sourcefiles", sourceFile.getName(), tenantId);
            log.info(String.format("Getting from %s", url));
            restTemplate.put(url, sourceFile);
        } catch (Exception e) {
            throw new RuntimeException("updateSourceFile: Remote call failure", e);
        }
    }

    public void sendPlsCreateModelEmail(String result, String tenantId) {
        try {
            String url = constructUrl("pls/internal/emails/createmodel/result", result, tenantId);
            log.info(String.format("Putting to %s", url));
            restTemplate.put(url, result);
        } catch (Exception e) {
            throw new RuntimeException("sendEmail: Remote call failure", e);
        }
    }
}
