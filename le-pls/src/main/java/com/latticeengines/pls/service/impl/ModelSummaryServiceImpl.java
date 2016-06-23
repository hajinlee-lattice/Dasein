package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@DisallowConcurrentExecution
@Component("modelSummaryService")
public class ModelSummaryServiceImpl implements ModelSummaryService {

    private static final Log log = LogFactory.getLog(ModelSummaryServiceImpl.class);

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Override
    public ModelSummary createModelSummary(String rawModelSummary, String tenantId) {
        ModelSummary modelSummary = modelSummaryParser.parse("", rawModelSummary);
        modelSummary.setUploaded(true);

        return createModelSummary(modelSummary, tenantId);
    }

    @Override
    public ModelSummary createModelSummary(ModelSummary modelSummary, String tenantId) {
        resolveNameIdConflict(modelSummary, tenantId);

        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        modelSummary.setTenant(tenant);

        if (modelSummary.getConstructionTime() == null) {
            modelSummary.setConstructionTime(System.currentTimeMillis());
        }
        modelSummary.setLastUpdateTime(modelSummary.getConstructionTime());
        modelSummaryEntityMgr.create(modelSummary);

        return modelSummary;
    }

    public boolean modelIdinTenant(String modelId, String tenantId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findByModelId(modelId, false, false, false);
        if (modelSummary == null) {
            return false;
        }
        Tenant tenant = modelSummary.getTenant();
        return (tenant != null) && tenantId.equals(tenant.getId());
    }

    private void resolveNameIdConflict(ModelSummary modelSummary, String tenantId) {
        List<ModelSummary> modelSummaries = modelSummaryEntityMgr.getAll();
        List<String> existingNames = new ArrayList<>();
        List<String> existingIds = new ArrayList<>();
        for (ModelSummary summary : modelSummaries) {
            if (summary.getTenant().getId().equals(tenantId)) {
                existingNames.add(summary.getName());
            }
            existingIds.add(summary.getId());
        }
        int version = 0;
        String possibleName = modelSummary.getName();
        String possibleId = modelSummary.getId();
        String rootId = possibleId;
        String rootname = modelSummaryParser.parseOriginalName(modelSummary.getName());
        while (existingNames.contains(possibleName) || existingIds.contains(possibleId)) {
            possibleName = modelSummary.getName().replace(rootname, rootname + "-" + String.format("%03d", ++version));
            possibleId = rootId + "-" + String.format("%03d", version);
        }

        if (version > 0) {
            log.info(String.format("Change model name from \"%s\" to \"%s\" to avoid conflicts.",
                    modelSummary.getName(), possibleName));
            log.info(String.format("Change model id from \"%s\" to \"%s\" to avoid conflicts.", modelSummary.getId(),
                    possibleId));
        }

        modelSummary.setId(possibleId);
        modelSummary.setName(possibleName);
    }

    public void updatePredictors(String modelId, AttributeMap attrMap) {
        if (modelId == null) {
            throw new NullPointerException("ModelId should not be null when updating the predictors");
        }
        if (attrMap == null) {
            throw new NullPointerException("Attribute Map should not be null when updating the predictors");
        }
        ModelSummary summary = modelSummaryEntityMgr.findByModelId(modelId, true, false, true);
        if (summary == null) {
            throw new NullPointerException("ModelSummary should not be null when updating the predictors");
        }
        List<Predictor> predictors = summary.getPredictors();
        modelSummaryEntityMgr.updatePredictors(predictors, attrMap);
    }

    @Override
    public ModelSummary getModelSummaryByModelId(String modelId) {
        return modelSummaryEntityMgr.getByModelId(modelId);
    }

    @Override
    public ModelSummary getModelSummaryEnrichedByDetails(String modelId) {
        ModelSummary summary = modelSummaryEntityMgr.findByModelId(modelId, false, true, true);
        if (summary != null) {
            summary.setPredictors(new ArrayList<Predictor>());
            summary.setDetails(null);
        }
        return summary;
    }

}
