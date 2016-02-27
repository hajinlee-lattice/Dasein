package com.latticeengines.scoringapi.controller;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;


public class InternalResourceRestApiProxy extends BaseRestApiProxy {

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
            return restTemplate.getForObject(constructUrl("pls/internal/modelsummaries/active", customerSpace.toString()), List.class);
        } catch (Exception e) {
            throw new RuntimeException("getActiveModelSummaries: Remote call failure", e);
        }
    }

    public ModelSummary getModelSummaryFromModelId(String modelId, CustomerSpace customerSpace) {
        try {
            return restTemplate.getForObject(constructUrl("pls/internal/modelsummaries/modelid", modelId, customerSpace.toString()),
                    ModelSummary.class);
        } catch (Exception e) {
            throw new RuntimeException("getModelSummaryFromModelId: Remote call failure", e);
        }
    }

}