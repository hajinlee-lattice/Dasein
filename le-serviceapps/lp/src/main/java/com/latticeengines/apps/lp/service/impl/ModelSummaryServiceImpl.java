package com.latticeengines.apps.lp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.apps.lp.service.ModelSummaryCacheService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.common.exposed.util.VersionComparisonUtils;
import com.latticeengines.db.exposed.entitymgr.KeyValueEntityMgr;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.AttributeMap;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.KeyValue;

@Component("modelSummaryService")
public class ModelSummaryServiceImpl implements ModelSummaryService {

    public static final Logger log = LoggerFactory.getLogger(ModelSummaryServiceImpl.class);
    public static final String PREDICTORS = "Predictors";
    public static final String ELEMENTS = "Elements";
    public static final String VALUES = "Values";
    public static final String BUSINESS_ANNUAL_SALES_ABS = "BusinessAnnualSalesAbs";
    public static final String CATEGORY = "Category";
    public static final String ACCOUNT_CATEGORY_ISSUE_FIXED = "AccountCategoryIssueFixed";
    public static final String REVENUE_UI_ISSUE_FIXED = "RevenueUIIssueFixed";
    public static final String NAME = "Name";
    public static final String DISPLAY_NAME = "DisplayName";
    public static final String LOWER_INCLUSIVE = "LowerInclusive";
    public static final String UPPER_EXCLUSIVE = "UpperExclusive";
    public static final String NO_PREDICTORS_WITH_MORE_THAN_200_DISTINCTVALUES = "NoPredictorsWithMoreThan200DistinctValues";
    public static final String LATTICE_GT200_DISCRETE_VALUE = "LATTICE_GT200_DiscreteValue";

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private KeyValueEntityMgr keyValueEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private ModelSummaryParser modelSummaryParser;

    @Inject
    private SourceFileService sourceFileService;

    @Inject
    private BucketedScoreService bucketedScoreService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private FeatureImportanceParser featureImportanceParser;

    @Inject
    private ModelSummaryCacheService modelSummaryCacheService;

    @Autowired
    @Qualifier("commonTaskExecutor")
    private ThreadPoolTaskExecutor modelSummaryDownloadExecutor;

    @Inject
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @Inject
    private AttrConfigService lpAttrConfigService;

    @Override
    public void create(ModelSummary summary) {
        modelSummaryEntityMgr.create(summary);
    }

    @Override
    public ModelSummary getModelSummaryByModelId(String modelId) {
        return modelSummaryEntityMgr.getByModelId(modelId);
    }

    @Override
    public ModelSummary findValidByModelId(String modelId) {
        return modelSummaryEntityMgr.findValidByModelId(modelId);
    }

    @Override
    public ModelSummary findByModelId(String modelId, boolean returnRelational, boolean returnDocument,
                                      boolean validOnly) {
        return modelSummaryEntityMgr.findByModelId(modelId, returnRelational, returnDocument, validOnly);
    }

    @Override
    public ModelSummary findByApplicationId(String applicationId) {
        return modelSummaryEntityMgr.findByApplicationId(applicationId);
    }

    @Override
    public List<ModelSummary> getModelSummariesByApplicationId(String applicationId) {
        return modelSummaryEntityMgr.getModelSummariesByApplicationId(applicationId);
    }

    @Override
    public void deleteByModelId(String modelId) {
        modelSummaryEntityMgr.deleteByModelId(modelId);
    }

    @Override
    public void delete(ModelSummary modelSummary) {
        modelSummaryEntityMgr.delete(modelSummary);
    }

    @Override
    public List<ModelSummary> getAll() {
        return modelSummaryEntityMgr.getAll();
    }

    @Override
    public List<String> getAllModelSummaryIds() {
        return modelSummaryEntityMgr.getAllModelSummaryIds();
    }

    @Override
    public List<ModelSummary> getAllByTenant(Tenant tenant) {
        return modelSummaryEntityMgr.getAllByTenant(tenant);
    }

    @Override
    public List<ModelSummary> findAllValid() {
        return modelSummaryEntityMgr.findAllValid();
    }

    @Override
    public List<ModelSummary> findAllActive() {
        return modelSummaryEntityMgr.findAllActive();
    }

    @Override
    public int findTotalCount(long lastUpdateTime, boolean considerAllStatus) {
        return modelSummaryEntityMgr.findTotalCount(lastUpdateTime, considerAllStatus);
    }

    @Override
    public List<ModelSummary> findPaginatedModels(long lastUpdateTime, boolean considerAllStatus, int offset,
                                                  int maximum) {
        return modelSummaryEntityMgr.findPaginatedModels(lastUpdateTime, considerAllStatus, offset, maximum);
    }

    @Override
    public void updateStatusByModelId(String modelId, ModelSummaryStatus status) {
        modelSummaryEntityMgr.updateStatusByModelId(modelId, status);
    }

    @Override
    public void updateModelSummary(ModelSummary modelSummary, AttributeMap attrMap) {
        modelSummaryEntityMgr.updateModelSummary(modelSummary, attrMap);
    }

    @Override
    public ModelSummary retrieveByModelIdForInternalOperations(String modelId) {
        return modelSummaryEntityMgr.retrieveByModelIdForInternalOperations(modelId);
    }

    @Override
    public void updatePredictors(List<Predictor> predictors, AttributeMap attrMap) {
        modelSummaryEntityMgr.updatePredictors(predictors, attrMap);
    }

    @Override
    public List<Predictor> findAllPredictorsByModelId(String modelId) {
        return modelSummaryEntityMgr.findAllPredictorsByModelId(modelId);
    }

    @Override
    public List<Predictor> findPredictorsUsedByBuyerInsightsByModelId(String modelId) {
        return modelSummaryEntityMgr.findPredictorsUsedByBuyerInsightsByModelId(modelId);
    }

    @Override
    public List<ModelSummary> getModelSummariesModifiedWithinTimeFrame(long timeFrame) {
        return modelSummaryEntityMgr.getModelSummariesModifiedWithinTimeFrame(timeFrame);
    }

    @Override
    public void updateLastUpdateTime(String modelId) {
        modelSummaryEntityMgr.updateLastUpdateTime(modelId);
    }

    @Override
    public boolean hasBucketMetadata(String modelId) {
        return modelSummaryEntityMgr.hasBucketMetadata(modelId);
    }

    @Override
    public List<ModelSummary> findModelSummariesByIds(Set<String> ids) {
        return modelSummaryEntityMgr.findModelSummariesByIds(ids);
    }

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
        ModelSummary modelSummary = findByModelId(modelId, false, false, false);
        if (modelSummary == null) {
            return false;
        }
        Tenant tenant = modelSummary.getTenant();
        return (tenant != null) && tenantId.equals(tenant.getId());
    }

    private void resolveNameIdConflict(ModelSummary modelSummary, String tenantId) {
        List<ModelSummary> modelSummaries = modelSummaryEntityMgr.findAll();
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

        ModelSummary dupModelSummary = getModelSummaryByModelId(possibleId);
        if (dupModelSummary != null && !existingIds.contains(dupModelSummary.getId())) {
            existingIds.add(dupModelSummary.getId());
        }
        while (existingNames.contains(possibleName) || existingIds.contains(possibleId)) {
            possibleName = modelSummary.getName().replace(rootname, rootname + "-" + String.format("%03d", ++version));
            possibleId = rootId + "-" + String.format("%03d", version);
            if (!existingIds.contains(possibleId) && getModelSummaryByModelId(possibleId) != null) {
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
        ModelSummary summary = findByModelId(modelId, true, false, true);
        if (summary == null) {
            throw new NullPointerException("ModelSummary should not be null when updating the predictors");
        }
        List<Predictor> predictors = summary.getPredictors();
        updatePredictors(predictors, attrMap);
    }

    @Override
    public ModelSummary getModelSummaryEnrichedByDetails(String modelId) {
        return findValidByModelId(modelId);
    }

    @Override
    public ModelSummary getModelSummary(String modelId) {
        ModelSummary summary = findValidByModelId(modelId);
        if (summary != null) {
            summary.setPredictors(new ArrayList<>());
            getModelSummaryTrainingFileState(summary);
            getModelSummaryHasRating(summary);
            if (!summary.getModelType().equals(ModelType.PMML.getModelType())) {
                fixBusinessAnnualSalesAbs(summary);
                fixLATTICEGT200DiscreteValue(summary);
                String sourceSchemaInterpretationStr = summary.getSourceSchemaInterpretation();
                if (sourceSchemaInterpretationStr != null
                        && sourceSchemaInterpretationStr.equals(SchemaInterpretation.SalesforceAccount.toString())) {
                    fixAccountCategory(summary);
                }
                fixCustomDisplayNames(summary);
            }
        }

        return summary;
    }

    private void fixCustomDisplayNames(ModelSummary summary) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            Map<String, String> nameToDisplayNameMap = findNameToDisplayNameMap();
            if (MapUtils.isNotEmpty(nameToDisplayNameMap)) {
                fixPredictorDisplayNameForModel(summary, nameToDisplayNameMap);
            }
            String msg = String.format("Fix custom displayNames for %s", summary.getId());
            timer.setTimerMessage(msg);
        }
    }

    private Map<String, String> findNameToDisplayNameMap() {
        Map<String, String> nameToDisplayNameMap = new HashMap<>();
        try {
            Map<BusinessEntity, List<AttrConfig>> customDisplayNameAttrs = lpAttrConfigService
                    .findAllHaveCustomDisplayNameByTenantId(MultiTenantContext.getShortTenantId());
            if (MapUtils.isNotEmpty(customDisplayNameAttrs)
                    && customDisplayNameAttrs.containsKey(BusinessEntity.Account)) {
                customDisplayNameAttrs.get(BusinessEntity.Account).forEach(config -> {
                    nameToDisplayNameMap.put(config.getAttrName(),
                            (String) config.getProperty(ColumnMetadataKey.DisplayName).getCustomValue());
                });

            }
        } catch (LedpException e) {
            log.warn("Got LedpException " + ExceptionUtils.getStackTrace(e));
        }
        return nameToDisplayNameMap;
    }

    private void fixPredictorDisplayNameForModel(ModelSummary summary, Map<String, String> nameToDisplayNameMap) {
        log.info("start to replace edit name for " + summary.getId());
        log.info("nameToDisplayNameMap: " + nameToDisplayNameMap);
        ObjectMapper objectMapper = new ObjectMapper();
        KeyValue keyValue = summary.getDetails();
        JsonNode details = null;
        try {
            details = objectMapper.readTree(keyValue.getPayload());
        } catch (IOException e) {
            log.error("Failed to parse model details KeyValue", e);
        }

        ArrayNode predictorsNodeOrig = (ArrayNode) details.get(PREDICTORS);
        for (JsonNode predictorNode : predictorsNodeOrig) {
            String name = predictorNode.get(NAME).asText();
            if (!nameToDisplayNameMap.containsKey(name)) {
                continue;
            }
            ((ObjectNode) predictorNode).put(DISPLAY_NAME, nameToDisplayNameMap.get(name));
        }
        keyValue.setPayload(details.toString());
        keyValueEntityMgr.update(keyValue);
    }

    @SuppressWarnings("deprecation")
    @VisibleForTesting
    void fixAccountCategory(ModelSummary summary) {
        ObjectMapper objectMapper = new ObjectMapper();
        KeyValue keyValue = summary.getDetails();
        JsonNode details = null;
        try {
            details = objectMapper.readTree(keyValue.getPayload());
        } catch (IOException e) {
            log.error("Failed to parse model details KeyValue", e);
        }
        JsonNode fixAccountCategoryIssue = details.get(ACCOUNT_CATEGORY_ISSUE_FIXED);
        if (fixAccountCategoryIssue != null) {
            return;
        }
        ArrayNode predictorsNodeOrig = (ArrayNode) details.get(PREDICTORS);
        ArrayNode predictorsNodeModified = JsonNodeFactory.instance.arrayNode();
        for (JsonNode predictorNode : predictorsNodeOrig) {
            String category = predictorNode.get(CATEGORY).asText();
            if (category != null && category.equals(Category.LEAD_INFORMATION.getName())) {
                ((ObjectNode) predictorNode).put(CATEGORY, Category.ACCOUNT_INFORMATION.getName());
            }
            predictorsNodeModified.add(predictorNode);
        }
        ((ObjectNode) details).put(PREDICTORS, predictorsNodeModified);
        ((ObjectNode) details).put(ACCOUNT_CATEGORY_ISSUE_FIXED, BooleanNode.TRUE);
        keyValue.setPayload(details.toString());
        keyValueEntityMgr.update(keyValue);
    }

    @SuppressWarnings("deprecation")
    private void fixBusinessAnnualSalesAbs(ModelSummary summary) {
        ObjectMapper objectMapper = new ObjectMapper();
        KeyValue keyValue = summary.getDetails();
        JsonNode details = null;
        try {
            details = objectMapper.readTree(keyValue.getPayload());
        } catch (IOException e) {
            log.error("Failed to parse model details KeyValue", e);
        }
        String dataCloudVersion = summary.getDataCloudVersion();
        JsonNode fixRevenueUIIssue = details.get(REVENUE_UI_ISSUE_FIXED);
        if (fixRevenueUIIssue != null
                || dataCloudVersion != null && VersionComparisonUtils.compareVersion(dataCloudVersion, "2.0.0") >= 0) {
            return;
        }
        ArrayNode predictors = (ArrayNode) details.get(PREDICTORS);
        for (JsonNode predictor : predictors) {
            if (!predictor.get(NAME).asText().equals(BUSINESS_ANNUAL_SALES_ABS)) {
                continue;
            }
            ArrayNode elements = (ArrayNode) predictor.get(ELEMENTS);
            for (JsonNode element : elements) {
                if (element.get(LOWER_INCLUSIVE).asText() != null) {
                    ((ObjectNode) element).put(LOWER_INCLUSIVE, element.get(LOWER_INCLUSIVE).asLong() * 1000);
                }
                if (element.get(UPPER_EXCLUSIVE).asText() != null) {
                    ((ObjectNode) element).put(UPPER_EXCLUSIVE, element.get(UPPER_EXCLUSIVE).asLong() * 1000);
                }
            }
        }
        ((ObjectNode) details).put(REVENUE_UI_ISSUE_FIXED, BooleanNode.TRUE);
        keyValue.setPayload(details.toString());
        keyValueEntityMgr.update(keyValue);
    }

    private void fixLATTICEGT200DiscreteValue(ModelSummary summary) {
        ObjectMapper objectMapper = new ObjectMapper();
        KeyValue keyValue = summary.getDetails();
        ObjectNode details = null;
        try {
            details = (ObjectNode) objectMapper.readTree(keyValue.getPayload());
        } catch (IOException e) {
            log.error("Failed to parse model details KeyValue", e);
        }
        JsonNode noPredictorsWithMoreThan200DistinctValues = details
                .get(NO_PREDICTORS_WITH_MORE_THAN_200_DISTINCTVALUES);
        if (noPredictorsWithMoreThan200DistinctValues != null) {
            return;
        }
        ArrayNode predictorsNodeOrig = (ArrayNode) details.get(PREDICTORS);
        ArrayNode predictorsNodeModified = JsonNodeFactory.instance.arrayNode();
        for (JsonNode predictorNode : predictorsNodeOrig) {
            Boolean removePredictor = false;
            ArrayNode elements = (ArrayNode) predictorNode.get(ELEMENTS);
            for (JsonNode element : elements) {
                ArrayNode values = (ArrayNode) element.get(VALUES);
                for (JsonNode valueNode : values) {
                    String value = valueNode.asText();
                    if (value.equals(LATTICE_GT200_DISCRETE_VALUE) || value.equals(LATTICE_GT200_DISCRETE_VALUE)) {
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
        details.set(PREDICTORS, predictorsNodeModified);
        details.set(NO_PREDICTORS_WITH_MORE_THAN_200_DISTINCTVALUES, BooleanNode.TRUE);
        keyValue.setPayload(details.toString());
        keyValueEntityMgr.update(keyValue);
    }

    @Override
    public List<ModelSummary> getModelSummaries(String selection) {
        List<ModelSummary> summaries;
        if (selection != null && selection.equalsIgnoreCase("all")) {
            try {
                Tenant tenant = MultiTenantContext.getTenant();
                EntityListCache<ModelSummary> entityListCache = modelSummaryCacheService.getEntitiesAndNonExistEntitityIdsByTenant(tenant);
                summaries = entityListCache.getExistEntities();
                Set<String> nonExistIds = entityListCache.getNonExistIds();
                if (summaries.size() == 0) {
                    summaries = getAll();
                    if (summaries.size() > 0) {
                        modelSummaryCacheService.buildEntitiesCache(tenant);
                    }
                } else if (nonExistIds.size() > 0) {
                    summaries.addAll(findModelSummariesByIds(nonExistIds));
                    // need to rebuild cache here
                    modelSummaryCacheService.buildEntitiesCache(tenant);
                }
            } catch (Exception exception) {
                // redis cache is in building, so don't use the redis data
                summaries = getAll();
            }
        } else {
            summaries = findAllValid();
        }
        List<SourceFile> sourceFiles = sourceFileService.findAllSourceFiles();
        Set<String> trainingTableNames = new HashSet<>();
        if (CollectionUtils.isNotEmpty(sourceFiles)) {
            for (SourceFile file : sourceFiles) {
                if (file.getTableName() != null && !file.getTableName().isEmpty()) {
                    trainingTableNames.add(file.getTableName());
                }
            }
        }

        for (ModelSummary summary : summaries) {
            summary.setPredictors(new ArrayList<>());
            summary.setDetails(null);
            getModelSummaryHasRating(summary);
            getModelSummaryTrainingFileState(summary, trainingTableNames);
        }

        return summaries;
    }

    private void getModelSummaryTrainingFileState(ModelSummary summary) {
        if (summary.getTrainingTableName() == null || summary.getTrainingTableName().isEmpty()) {
            summary.setTrainingFileExist(false);
        } else {
            SourceFile sourceFile = sourceFileService.getByTableNameCrossTenant(summary.getTrainingTableName());
            if (sourceFile == null) {
                summary.setTrainingFileExist(false);
            } else {
                summary.setTrainingFileExist(true);
            }
        }
    }

    private void getModelSummaryTrainingFileState(ModelSummary summary, Set<String> trainingTableNames) {
        if (summary.getTrainingTableName() == null || summary.getTrainingTableName().isEmpty()) {
            summary.setTrainingFileExist(false);
        } else {
            summary.setTrainingFileExist(trainingTableNames.contains(summary.getTrainingTableName()));
        }
    }

    @Override
    public void updateModelSummary(String modelId, AttributeMap attrMap) {
        ModelSummary modelSummary = getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[]{modelId});
        }
        updateModelSummary(modelSummary, attrMap);
    }

    @Override
    public boolean downloadModelSummary(String tenantId, Map<String, String> modelApplicationIdToEventColumn) {
        boolean result = downloadModelSummaryForTenant(tenantId, modelApplicationIdToEventColumn);
        if (!result) {
            log.info("No result of downloading model summary for " //
                    + JsonUtils.serialize(modelApplicationIdToEventColumn));
            try {
                getEventToModelSummary(tenantId, modelApplicationIdToEventColumn);
                return true;
            } catch (Exception e) {
                log.error("Fail to invoke getEventToModelSummary" + e);
                return false;
            }
        }
        return true;
    }

    private boolean downloadModelSummaryForTenant(String tenantId,
                                                  Map<String, String> modelApplicationIdToEventColumn) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[]{tenantId});
        }

        Set<String> applicationFilters = null;
        if (modelApplicationIdToEventColumn != null) {
            applicationFilters = new HashSet<>();
            for (String modelApplicationId : modelApplicationIdToEventColumn.keySet()) {
                applicationFilters.add(ApplicationIdUtils.stripJobId(modelApplicationId));
            }
        }

        long startTime = System.currentTimeMillis();
        Set<String> modelSummaryIds = getModelSummaryIds();
        ModelDownloaderCallable.Builder builder = new ModelDownloaderCallable.Builder();
        builder.modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .tenant(tenant) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .bucketedScoreService(bucketedScoreService) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(modelSummaryParser) //
                .featureImportanceParser(featureImportanceParser) //
                .modelSummaryIds(modelSummaryIds) //
                .applicationFilters(applicationFilters);
        ModelDownloaderCallable callable = new ModelDownloaderCallable(builder);

        modelSummaryDownloadFlagEntityMgr.addExcludeFlag(tenantId);
        Boolean result;
        try {
            log.info(String.format("Model summary download executor submit, tenant: %s", tenantId));
            result = callable.call();
        } catch (Exception e) {
            log.error("ModelDownloaderCallable failed " + e);
            throw new RuntimeException(e);
        }
        modelSummaryDownloadFlagEntityMgr.removeExcludeFlag(tenantId);

        long totalSeconds = (System.currentTimeMillis() - startTime);
        log.info(String.format("Download for tenant %s duration: %dms", tenantId, totalSeconds));

        return result;
    }

    @Override
    public Set<String> getModelSummaryIds() {
        Set<String> modelSummaryIds = Collections.synchronizedSet(new HashSet<>());
        List<String> summaries = getAllModelSummaryIds();
        for (String summary : summaries) {
            try {
                modelSummaryIds.add(UuidUtils.extractUuid(summary));
            } catch (Exception e) {
                // Skip any model summaries that have unexpected ID syntax
                log.warn("Failed to add model summary id " + e.getMessage());
            }
        }
        return modelSummaryIds;
    }

    @Override
    public Map<String, ModelSummary> getEventToModelSummary(String tenantId,
                                                            Map<String, String> modelApplicationIdToEventColumn) {
        Map<String, ModelSummary> eventToModelSummary = new HashMap<>();
        Set<String> foundModels = new HashSet<>();

        int maxTries = 2;
        int i = 0;

        log.info("Expecting to retrieve models with these application ids:"
                + Joiner.on(", ").join(modelApplicationIdToEventColumn.keySet()));

        do {
            for (String modelApplicationId : modelApplicationIdToEventColumn.keySet()) {
                if (!foundModels.contains(modelApplicationId)) {
                    ModelSummary model = findByApplicationId(modelApplicationId);

                    if (model != null) {
                        eventToModelSummary.put(modelApplicationIdToEventColumn.get(modelApplicationId), model);
                        foundModels.add(modelApplicationId);
                    } else {
                        log.info("Did not find model summary with application id " + modelApplicationId + ", download now.");
                        downloadModelSummary(tenantId, modelApplicationIdToEventColumn);
                    }
                }
            }

            if (eventToModelSummary.size() < modelApplicationIdToEventColumn.size()) {
                for (String modelApplicationId : modelApplicationIdToEventColumn.keySet()) {
                    if (!foundModels.contains(modelApplicationId)) {
                        ModelSummary model = findByApplicationId(modelApplicationId);

                        if (model != null) {
                            eventToModelSummary.put(modelApplicationIdToEventColumn.get(modelApplicationId), model);
                            foundModels.add(modelApplicationId);
                        } else {
                            log.error(String.format("Model summary is not found by application id: %s",
                                    modelApplicationId));
                        }
                    }
                }
            }

            i++;

            if (i == maxTries) {
                break;
            }
        } while (eventToModelSummary.size() < modelApplicationIdToEventColumn.size());

        if (eventToModelSummary.size() < modelApplicationIdToEventColumn.size()) {
            Joiner joiner = Joiner.on(",").skipNulls();
            throw new LedpException(LedpCode.LEDP_28013,
                    new String[]{joiner.join(modelApplicationIdToEventColumn.keySet()), joiner.join(foundModels)});
        }

        return eventToModelSummary;
    }

    private void getModelSummaryHasRating(ModelSummary summary) {
        summary.setHasBucketMetadata(hasBucketMetadata(summary.getId()));
    }
}
