package com.latticeengines.modelquality.service;

public interface DataSetService {
    
    String createDataSetFromLP2Tenant(String tenantName, String modelID);
    
    String createDataSetFromLPITenant(String tenantName, String modelID);
    
    String createDataSetFromPlaymakerTenant(String tenantName, String playExternalID);

}
