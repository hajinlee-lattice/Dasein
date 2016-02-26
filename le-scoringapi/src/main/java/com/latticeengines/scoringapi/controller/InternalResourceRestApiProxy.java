package com.latticeengines.scoringapi.controller;

import java.util.List;

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

    public List<?> getActiveModelSummaries(String tenantId) {
        try {
            return restTemplate.getForObject(constructUrl("pls/internal/modelsummaries/active", tenantId), List.class);
        } catch (Exception e) {
            throw new RuntimeException("getActiveModelSummaries: Remote call failure", e);
        }
    }

}