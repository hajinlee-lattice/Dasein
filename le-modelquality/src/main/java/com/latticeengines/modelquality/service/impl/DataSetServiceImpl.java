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
public class DataSetServiceImpl extends BaseServiceImpl implements DataSetService {

    @Autowired
    DataSetEntityMgr dataSetEntityMgr;

    @Override
    public String createDataSetFromLP2Tenant(String tenantId, String modelId) {
        return createDataSetFromLPTenant(tenantId, modelId, "2.0");
    }

    @Override
    public String createDataSetFromLPITenant(String tenantId, String modelId) {
        return createDataSetFromLPTenant(tenantId, modelId, "3.0");
    }

    @Override
    public String createDataSetFromPlaymakerTenant(String tenantName, String playExternalId) {
        throw new NotImplementedException("Playmaker tenants are not yet supported");
    }

    private String createDataSetFromLPTenant(String tenantId, String modelId, String version) {

        ModelSummary modelSummary = internalResourceRestApiProxy.getModelSummaryFromModelId(modelId,
                CustomerSpace.parse(tenantId));
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_35005, new String[] { tenantId, modelId });
        }
        String trainingFilePath = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");

        if (trainingFilePath == null || trainingFilePath.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35005, new String[] { tenantId, modelId });
        }

        if (modelSummary.getSourceSchemaInterpretation() == null
                || modelSummary.getSourceSchemaInterpretation().isEmpty()) {
            throw new LedpException(LedpCode.LEDP_35006, new String[] { tenantId, modelId });
        }
        SchemaInterpretation schemaInterpretation = SchemaInterpretation
                .valueOf(modelSummary.getSourceSchemaInterpretation());

        DataSet dataset = dataSetEntityMgr.findByTenantAndTrainingSet(tenantId, trainingFilePath);
        if (dataset != null) {
            return dataset.getName();
        }

        dataset = new DataSet();
        dataset.setName(tenantId + "_" + modelId);
        dataset.setDataSetType(DataSetType.FILE);
        dataset.setSchemaInterpretation(schemaInterpretation);
        dataset.setTrainingSetHdfsPath(trainingFilePath);
        dataset.setIndustry("Unknown");
        Tenant tenant = new Tenant(tenantId);
        tenant.setUiVersion(version);
        dataset.setTenant(tenant);

        dataSetEntityMgr.create(dataset);
        return dataset.getName();
    }
}
