package com.latticeengines.modelquality.service;

import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public interface DataSetService {
    
    String createDataSetFromLP2Tenant(String tenantName, String modelID, SchemaInterpretation schemaInterpretation);
    
    String createDataSetFromLPITenant(String tenantName, String modelID, SchemaInterpretation schemaInterpretation);
    
    String createDataSetFromPlaymakerTenant(String tenantName, String playExternalID);

}
