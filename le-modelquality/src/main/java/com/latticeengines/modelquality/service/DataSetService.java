package com.latticeengines.modelquality.service;

public interface DataSetService {
    
    String createDataSetFromLP2Tenant(String tenantId, String modelId);
    
    String createDataSetFromLPITenant(String tenantId, String modelId);
    
    String createDataSetFromPlaymakerTenant(String tenantId, String playExternalId);

}
