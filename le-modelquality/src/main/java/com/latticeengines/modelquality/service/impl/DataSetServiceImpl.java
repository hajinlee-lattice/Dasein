package com.latticeengines.modelquality.service.impl;

import org.apache.directory.api.util.exception.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.service.DataSetService;

@Component("dataSetService")
public class DataSetServiceImpl extends BaseServiceImpl implements DataSetService{
    
    @Autowired
    DataSetEntityMgr dataSetEntityMgr;
    
    @Override
    public String createDataSetFromLP2Tenant(String tenantId, String modelID, SchemaInterpretation schemaInterpretation){
        return createDataSetFromLPTenant(tenantId, modelID, schemaInterpretation, "2.0");
    }
    
    @Override
    public String createDataSetFromLPITenant(String tenantId, String modelID, SchemaInterpretation schemaInterpretation){
        return createDataSetFromLPTenant(tenantId, modelID, schemaInterpretation, "3.0");
    }
    
    @Override
    public String createDataSetFromPlaymakerTenant(String tenantName, String playExternalID){
        throw new NotImplementedException("Playmaker tenants are not yet supported");
    }

    private String createDataSetFromLPTenant(String tenantId, String modelID, SchemaInterpretation schemaInterpretation,
            String string) {
        String trainingFileCsvPath = findTrainingFileCSVPath(tenantId, modelID);
        
        if(trainingFileCsvPath == null || trainingFileCsvPath.isEmpty()){
            throw new LedpException(LedpCode.LEDP_35005, new String[] {tenantId, modelID});
        }
        
        DataSet dataset = dataSetEntityMgr.findByTenantAndTrainingSet(tenantId, trainingFileCsvPath);
        if(dataset != null){
            return dataset.getName();
        }
        
        dataset = new DataSet();
        dataset.setName(tenantId+"_"+modelID);
        dataset.setDataSetType(DataSetType.FILE);
        dataset.setSchemaInterpretation(schemaInterpretation);
        dataset.setTrainingSetHdfsPath(trainingFileCsvPath);
        dataset.setIndustry("Unknown");
        Tenant tenant = new Tenant(tenantId);
        tenant.setUiVersion("2.0");
        dataset.setTenant(tenant);
        
        dataSetEntityMgr.create(dataset);
        return dataset.getName();
    }

    private String findTrainingFileCSVPath(String tenantId, String modelID) {
        ModelSummary modelSummary = internalResourceRestApiProxy.getModelSummaryFromModelId(modelID,
                CustomerSpace.parse(tenantId));
        if(modelSummary == null){
            return null;
        }
        
        String trainingFilePath = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");
        return trainingFilePath;
    }


}
