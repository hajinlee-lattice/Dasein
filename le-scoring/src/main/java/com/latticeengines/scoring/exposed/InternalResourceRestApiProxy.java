package com.latticeengines.scoring.exposed;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
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

    public List<?> getActiveModelSummaries(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/modelsummaries/active", customerSpace.toString());
            log.debug("Get from " + url);
            return restTemplate.getForObject(url, List.class);
        } catch (Exception e) {
            throw new RuntimeException("getActiveModelSummaries: Remote call failure", e);
        }
    }

    public ModelSummary getModelSummaryFromModelId(String modelId, CustomerSpace customerSpace) {
        ModelSummary modelSummary = null;
        try {
            String url = constructUrl("pls/internal/modelsummaries/modelid", modelId, customerSpace.toString());
            log.debug("Get from " + url);
            modelSummary = restTemplate.getForObject(url, ModelSummary.class);
        } catch (Exception e) {
            throw new RuntimeException("getModelSummaryFromModelId: Remote call failure", e);
        }
        return modelSummary;
    }

    public void createModelSummary(ModelSummary modelSummary, CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/modelsummaries", customerSpace.toString());
            log.debug(String.format("Posting to %s", url));
            restTemplate.postForObject(url, modelSummary, Void.class);
        } catch (Exception e) {
            throw new RuntimeException("createModelSummary: Remote call failure", e);
        }
    }

    public void deleteModelSummary(String modelId, CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/internal/modelsummaries/", modelId, customerSpace.toString());
            log.debug(String.format("Deleting to %s", url));
            restTemplate.delete(url);
        } catch (Exception e) {
            throw new RuntimeException("deleteModelSummary: Remote call failure", e);
        }
    }

    public boolean createTenant(Tenant tenant) {
        try {
            String url = constructUrl("pls/admin/tenants");
            log.debug(String.format("Posting to %s", url));
            return restTemplate.postForObject(url, tenant, Boolean.class);
        } catch (Exception e) {
            throw new RuntimeException("createTenant: Remote call failure", e);
        }
    }

    public void deleteTenant(CustomerSpace customerSpace) {
        try {
            String url = constructUrl("pls/admin/tenants/", customerSpace.toString());
            log.debug(String.format("Deleting to %s", url));
            restTemplate.delete(url);
        } catch (Exception e) {
            throw new RuntimeException("deleteTenant: Remote call failure", e);
        }
    }

    public int getModelsCount(CustomerSpace customerSpace, String start, boolean considerAllStatus) {
        try {
            String url = constructUrl("pls/internal/modelsummarydetails/count", customerSpace.toString());
            url += "?" + "considerAllStatus" + "=" + considerAllStatus;
            if (!StringUtils.isEmpty(start)) {
                url += "&" + "start" + "=" + start;
            }
            log.debug("Get from " + url);
            return restTemplate.getForObject(url, Integer.class);
        } catch (Exception e) {
            throw new RuntimeException("getModelsCount: Remote call failure: " + e.getMessage(), e);
        }
    }

    public List<?> getPaginatedModels(CustomerSpace customerSpace, String start, int offset, int maximum,
            boolean considerAllStatus) {
        try {
            String url = constructUrl("pls/internal/modelsummarydetails/paginate", customerSpace.toString());
            url += "?" + "considerAllStatus" + "=" + considerAllStatus + "&" + "offset" + "=" + offset + "&"
                    + "maximum" + "=" + maximum;
            if (!StringUtils.isEmpty(start)) {
                url += "&" + "start" + "=" + start;
            }

            log.debug("Get from " + url);
            return restTemplate.getForObject(url, List.class);
        } catch (Exception e) {
            throw new RuntimeException("getPaginatedModels: Remote call failure: " + e.getMessage(), e);
        }
    }

}