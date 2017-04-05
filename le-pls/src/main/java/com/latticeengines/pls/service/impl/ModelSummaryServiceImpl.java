package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.VersionComparisonUtils;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.KeyValueEntityMgr;

@DisallowConcurrentExecution
@Component("modelSummaryService")
public class ModelSummaryServiceImpl implements ModelSummaryService {

    private static final Log log = LogFactory.getLog(ModelSummaryServiceImpl.class);

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private KeyValueEntityMgr keyValueEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

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

    @Override
    public boolean modelIdinTenant(String modelId, String tenantId) {
        ModelSummary modelSummary = modelSummaryEntityMgr.findByModelId(modelId, false, false, false);
        if (modelSummary == null) {
            return false;
        }
        Tenant tenant = modelSummary.getTenant();
        return (tenant != null) && tenantId.equals(tenant.getId());
    }

    private void resolveNameIdConflict(ModelSummary modelSummary, String tenantId) {
        List<ModelSummary> modelSummaries = modelSummaryEntityMgr.findAll();
        List<String> existingNames = new ArrayList<String>();
        List<String> existingIds = new ArrayList<String>();
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

        ModelSummary dupModelSummary = modelSummaryEntityMgr.getByModelId(possibleId);
        if (dupModelSummary != null && !existingIds.contains(dupModelSummary.getId())) {
            existingIds.add(dupModelSummary.getId());
        }
        while (existingNames.contains(possibleName) || existingIds.contains(possibleId)) {
            possibleName = modelSummary.getName().replace(rootname, rootname + "-" + String.format("%03d", ++version));
            possibleId = rootId + "-" + String.format("%03d", version);
            if (!existingIds.contains(possibleId) && modelSummaryEntityMgr.getByModelId(possibleId) != null) {
                existingIds.add(possibleId);
            }
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

    @Override
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

    @Override
    public List<ModelSummary> getAllByTenant(Tenant tenant) {
        return modelSummaryEntityMgr.getAllByTenant(tenant);
    }

    @Override
    public ModelSummary getModelSummary(String modelId) {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        if (summary != null) {
            summary.setPredictors(new ArrayList<Predictor>());
            getModelSummaryTrainingFileState(summary);
            if (!summary.getModelType().equals(ModelType.PMML.getModelType())) {
                fixBusinessAnnualSalesAbs(summary);
                fixLATTICEGT200DiscreteValue(summary);
            }
        }
        return summary;
    }

    public void fixBusinessAnnualSalesAbs(ModelSummary summary) {
        ObjectMapper objectMapper = new ObjectMapper();
        KeyValue keyValue = summary.getDetails();
        JsonNode details = null;
        try {
            details = objectMapper.readTree(keyValue.getPayload());
        } catch (IOException e) {
            log.error("Failed to parse model details KeyValue", e);
        }
        String dataCloudVersion = summary.getDataCloudVersion();
        JsonNode fixRevenueUIIssue = details.get("RevenueUIIssueFixed");
        if (fixRevenueUIIssue != null
                || dataCloudVersion != null && VersionComparisonUtils.compareVersion(dataCloudVersion, "2.0.0") >= 0) {
            return;
        }
        ArrayNode predictors = (ArrayNode) details.get("Predictors");
        for (JsonNode predictor : predictors) {
            if (!predictor.get("Name").asText().equals("BusinessAnnualSalesAbs")) {
                continue;
            }
            ArrayNode elements = (ArrayNode) predictor.get("Elements");
            for (JsonNode element : elements) {
                if (element.get("LowerInclusive").asText() != null) {
                    ((ObjectNode) element).put("LowerInclusive", element.get("LowerInclusive").asLong() * 1000);
                }
                if (element.get("UpperExclusive").asText() != null) {
                    ((ObjectNode) element).put("UpperExclusive", element.get("UpperExclusive").asLong() * 1000);
                }
            }
        }
        ((ObjectNode) details).put("RevenueUIIssueFixed", BooleanNode.TRUE);
        keyValue.setPayload(details.toString());
        keyValueEntityMgr.update(keyValue);
    }

    public void fixLATTICEGT200DiscreteValue(ModelSummary summary) {
        ObjectMapper objectMapper = new ObjectMapper();
        KeyValue keyValue = summary.getDetails();
        ObjectNode details = null;
        try {
            details = (ObjectNode) objectMapper.readTree(keyValue.getPayload());
        } catch (IOException e) {
            log.error("Failed to parse model details KeyValue", e);
        }
        JsonNode noPredictorsWithMoreThan200DistinctValues = details.get("NoPredictorsWithMoreThan200DistinctValues");
        if (noPredictorsWithMoreThan200DistinctValues != null) {
            return;
        }
        ArrayNode predictorsNodeOrig = (ArrayNode) details.get("Predictors");
        ArrayNode predictorsNodeModified = JsonNodeFactory.instance.arrayNode();
        for (JsonNode predictorNode : predictorsNodeOrig) {
            Boolean removePredictor = false;
            ArrayNode elements = (ArrayNode) predictorNode.get("Elements");
            for (JsonNode element : elements) {
                ArrayNode values = (ArrayNode) element.get("Values");
                for (JsonNode valueNode : values) {
                    String value = valueNode.asText();
                    if (value.equals("LATTICE_GT200_DiscreteValue") || value.equals("LATTICE_GT200_DistinctValue")) {
                        removePredictor = true;
                        break;
                    }
                }
                if (removePredictor) {
                    break;
                }
            }
            if (!removePredictor) {
                predictorsNodeModified.add(predictorNode);
            }
        }
        details.put("Predictors", predictorsNodeModified);
        details.put("NoPredictorsWithMoreThan200DistinctValues", BooleanNode.TRUE);
        keyValue.setPayload(details.toString());
        keyValueEntityMgr.update(keyValue);
    }

    @Override
    public ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument,
            boolean validOnly) {
        return modelSummaryEntityMgr.findByModelId(modelId, returnRelational, returnDocument, validOnly);
    }

    @Override
    public List<ModelSummary> getModelSummaries(String selection) {
        List<ModelSummary> summaries;
        if (selection != null && selection.equalsIgnoreCase("all")) {
            summaries = modelSummaryEntityMgr.findAll();
        } else {
            summaries = modelSummaryEntityMgr.findAllValid();
        }

        for (ModelSummary summary : summaries) {
            summary.setPredictors(new ArrayList<Predictor>());
            summary.setDetails(null);
            getModelSummaryTrainingFileState(summary);
        }
        return summaries;
    }

    private void getModelSummaryTrainingFileState(ModelSummary summary) {
        if (summary.getTrainingTableName() == null || summary.getTrainingTableName().isEmpty()) {
            summary.setTrainingFileExist(false);
        } else {
            SourceFile sourceFile = sourceFileEntityMgr.getByTableName(summary.getTrainingTableName());
            if (sourceFile == null) {
                summary.setTrainingFileExist(false);
            } else {
                summary.setTrainingFileExist(true);
            }
        }
    }
}
