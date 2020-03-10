package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.rating.CrossSellRatingQueryBuilder;
import com.latticeengines.apps.cdl.rating.RatingQueryBuilder;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.cdl.util.CustomEventModelingDataStoreUtil;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.util.FeatureImportanceMgr;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStoreName;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.AdvancedModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataStoreProxy;

import reactor.core.publisher.Flux;

@Component("aiModelService")
public class AIModelServiceImpl extends RatingModelServiceBase<AIModel> implements AIModelService {

    private static final Logger log = LoggerFactory.getLogger(AIModelServiceImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private SegmentService segmentService;

    @Inject
    private AIModelEntityMgr aiModelEntityMgr;

    @Inject
    private PeriodService periodService;

    @Inject
    private MetadataStoreProxy metadataStoreProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private FeatureImportanceMgr featureImportanceUtil;

    @Inject
    private BatonService batonService;

    private static RatingEngineType[] types = //
            new RatingEngineType[] { //
                    RatingEngineType.CROSS_SELL, //
                    RatingEngineType.CUSTOM_EVENT };

    private final String statsCubeKey = "Account";
    private final String nullLabel = "Not Populated";

    protected AIModelServiceImpl() {
        super(Arrays.asList(types));
    }

    @Override
    public List<AIModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return aiModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
    }

    @Override
    public AIModel getRatingModelById(String id) {
        return aiModelEntityMgr.findById(id);
    }

    @Override
    public AIModel createOrUpdate(AIModel ratingModel, String ratingEngineId) {
        if (ratingModel.getTrainingSegment() != null) {
            String segmentName = ratingModel.getTrainingSegment().getName();
            MetadataSegment segment = segmentService.findByName(segmentName);
            segment.setPid(segment.getPid());
            ratingModel.setTrainingSegment(segment);
        }
        if (ratingModel.getId() == null) {
            ratingModel.setId(AIModel.generateIdStr());
            log.info(String.format("Creating an AI model with id %s for ratingEngine %s", ratingModel.getId(),
                    ratingEngineId));
            return aiModelEntityMgr.createAIModel(ratingModel, ratingEngineId);
        } else {
            AIModel retrievedAIModel = aiModelEntityMgr.findById(ratingModel.getId());
            if (retrievedAIModel == null) {
                log.warn(String.format("AIModel with id %s is not found. Creating a new one", ratingModel.getId()));
                return aiModelEntityMgr.createAIModel(ratingModel, ratingEngineId);
            } else {
                return aiModelEntityMgr.updateAIModel(ratingModel, retrievedAIModel, ratingEngineId);
            }
        }
    }

    @Override
    public AIModel createNewIteration(AIModel aiModel, RatingEngine ratingEngine) {
        String customerSpace = CustomerSpace
                .shortenCustomerSpace(CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString());
        if (StringUtils.isEmpty(aiModel.getDerivedFromRatingModel())) {
            throw new LedpException(LedpCode.LEDP_40039, new String[] { MultiTenantContext.getTenant().getId() });
        }

        AIModel derivedFromRatingModel = getRatingModelById(aiModel.getDerivedFromRatingModel());
        if (!derivedFromRatingModel.getRatingEngine().getId().equals(ratingEngine.getId())) {
            throw new LedpException(LedpCode.LEDP_40040, new String[] { MultiTenantContext.getTenant().getId() });
        }

        if (derivedFromRatingModel.getModelingJobStatus() != JobStatus.COMPLETED) {
            throw new LedpException(LedpCode.LEDP_40058, new String[] { MultiTenantContext.getTenant().getId() });
        }

        AIModel toCreate = new AIModel();
        toCreate.setPredictionType(derivedFromRatingModel.getPredictionType());
        toCreate.setCreatedBy(aiModel.getCreatedBy());
        toCreate.setUpdatedBy(aiModel.getUpdatedBy());
        toCreate.setRatingEngine(ratingEngine);
        toCreate.setTrainingSegment(aiModel.getTrainingSegment());
        toCreate.setAdvancedModelingConfig(aiModel.getAdvancedModelingConfig());
        toCreate.setDerivedFromRatingModel(derivedFromRatingModel.getId());

        if (ratingEngine.getType() == RatingEngineType.CUSTOM_EVENT) {
            log.info("Cloning the Sourcefile and Training table for the new iteration");
            CustomEventModelingConfig modelingConfig = (CustomEventModelingConfig) toCreate.getAdvancedModelingConfig();
            String sourceFileName = modelingConfig.getSourceFileName();
            SourceFile originalSourceFile = sourceFileProxy.findByName(customerSpace,
                    modelingConfig.getSourceFileName());
            // Remove this below line and replace it with some code that creates
            // a metadata table but does not copy the
            // underlying data
            Table clonedTable = metadataProxy.cloneTable(customerSpace, originalSourceFile.getTableName(), true);
            sourceFileProxy.copySourceFile(customerSpace, sourceFileName, clonedTable.getName(), customerSpace);
            SourceFile clonedSourceFile = sourceFileProxy.findByTableName(customerSpace, clonedTable.getName());
            modelingConfig.setSourceFileName(clonedSourceFile.getName());
            modelingConfig.setSourceFileDisplayName(clonedSourceFile.getDisplayName());
            log.info("Completed cloning the Sourcefile and Training table for the new iteration");
        }

        return createOrUpdate(toCreate, ratingEngine.getId());
    }

    @Override
    public void deleteById(String id) {
        aiModelEntityMgr.deleteById(id);
    }

    @Override
    public EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version) {
        CrossSellModelingConfig advancedConf = (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();
        Set<String> attributeMetadata = servingStoreProxy
                .getDecoratedMetadataFromCache(customerSpace, BusinessEntity.Account).stream()
                .filter(ColumnMetadata::isDateAttribute).map(ColumnMetadata::getAttrName).collect(Collectors.toSet());

        if (CollectionUtils.isEmpty(attributeMetadata)) {
            log.warn("No metadata attribute metadata found for tenant: " + customerSpace);
            // Initilize an empty map so that modeling query generation does not
            // fail
            attributeMetadata = new HashSet<>();
        }

        if (advancedConf != null
                && Arrays.asList(ModelingStrategy.values()).contains(advancedConf.getModelingStrategy())) {
            PeriodStrategy strategy = periodService.getApsRollupPeriod(version);
            int maxPeriod = periodService.getMaxPeriodId(customerSpace, strategy, version);
            RatingQueryBuilder ratingQueryBuilder = CrossSellRatingQueryBuilder.getCrossSellRatingQueryBuilder(
                    ratingEngine, aiModel, modelingQueryType, strategy.getName(), maxPeriod, attributeMetadata);
            return ratingQueryBuilder.build();
        } else {
            throw new LedpException(LedpCode.LEDP_40009, new String[] { ratingEngine.getId(), aiModel.getId(),
                    ratingEngine.getType().name(), customerSpace });
        }
    }

    @Override
    public void findRatingModelAttributeLookups(AIModel ratingModel) {
        List<MetadataSegment> segments = new ArrayList<>();
        if (ratingModel.getTrainingSegment() != null) {
            segments.add(ratingModel.getTrainingSegment());
        }
        boolean shouldHaveParentSegment = true;
        AdvancedModelingConfig advancedModelingConfig = ratingModel.getAdvancedModelingConfig();
        if (advancedModelingConfig instanceof CustomEventModelingConfig) {
            CustomEventModelingConfig customEventModelingConfig = (CustomEventModelingConfig) advancedModelingConfig;
            if (CustomEventModelingType.LPI.equals(customEventModelingConfig.getCustomEventModelingType())) {
                shouldHaveParentSegment = false;
            }
        }
        if (shouldHaveParentSegment) {
            MetadataSegment segment = aiModelEntityMgr.inflateParentSegment(ratingModel);
            if (segment != null) {
                segments.add(segment);
            }
        }
        if (CollectionUtils.isNotEmpty(segments)) {
            ratingModel.setRatingModelAttributes(new HashSet<>(segmentService.findDependingAttributes(segments)));
        }
    }

    @Override
    public void updateModelingJobStatus(String customerSpace, String ratingEngineId, String aiModelId,
            JobStatus newStatus) {
        AIModel aiModel = getRatingModelById(aiModelId);
        if (aiModel.getModelingJobStatus().isTerminated()) {
            throw new LedpException(LedpCode.LEDP_40028, new String[] { aiModelId });
        }
        aiModel.setModelingJobStatus(newStatus);
        createOrUpdate(aiModel, ratingEngineId);
        log.info(String.format("Modeling Job status updated for AIModel:%s, RatingEngine:%s to %s", aiModelId,
                ratingEngineId, newStatus.name()));
    }

    @Override
    public List<ColumnMetadata> getIterationMetadata(String customerSpace, RatingEngine ratingEngine, AIModel aiModel,
            List<CustomEventModelingConfig.DataStore> dataStores) {
        return new ArrayList<>(getIterationMetadataAsMap(customerSpace, ratingEngine, aiModel, dataStores).values());
    }

    private Map<String, ColumnMetadata> getIterationMetadataAsMap(String customerSpace, RatingEngine ratingEngine,
            AIModel aiModel, List<CustomEventModelingConfig.DataStore> dataStores) {
        StopWatch stopWatch = new StopWatch();
        StopWatch splitter = new StopWatch();

        log.info("Iteration metadata compilation(Split: 0 ms Total: 0 ms): Start");

        stopWatch.start();
        splitter.start();

        if (aiModel.getModelingJobStatus() == null || !aiModel.getModelingJobStatus().isTerminated()) {
            throw new LedpException(LedpCode.LEDP_40034,
                    new String[] { aiModel.getId(), ratingEngine.getId(), customerSpace });
        }
        if (aiModel.getModelingJobStatus() != JobStatus.COMPLETED || StringUtils.isEmpty(aiModel.getModelSummaryId())) {
            throw new LedpException(LedpCode.LEDP_40035,
                    new String[] { aiModel.getId(), ratingEngine.getId(), customerSpace });
        }
        ModelSummary modelSummary = modelSummaryProxy.getByModelId(aiModel.getModelSummaryId());
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_40036,
                    new String[] { "ModelSummary", aiModel.getId(), ratingEngine.getId(), customerSpace });
        }
        String tableName = modelSummary.getEventTableName();
        if (tableName == null) {
            throw new LedpException(LedpCode.LEDP_40036,
                    new String[] { "Event table name", aiModel.getId(), ratingEngine.getId(), customerSpace });
        }

        Table table = metadataProxy.getTable(customerSpace, tableName);
        if (table == null) {
            throw new LedpException(LedpCode.LEDP_40036,
                    new String[] { "Event table metadata", aiModel.getId(), ratingEngine.getId(), customerSpace });
        }

        log.info(String.format(
                "Iteration metadata compilation (Split: %d ms Total: %d ms): Validation & Object Procurement",
                splitter.getTime(), stopWatch.getTime()));
        stopWatch.suspend();
        splitter.reset();

        stopWatch.resume();
        splitter.start();
        Map<String, Predictor> predictors = extractPredictorsFromSummary(modelSummary);
        log.info(String.format("Iteration metadata compilation (Split: %d ms Total: %d ms): Predictor extraction",
                splitter.getTime(), stopWatch.getTime()));
        stopWatch.suspend();
        splitter.reset();

        Set<Category> selectedCategories = CollectionUtils.isEmpty(dataStores)
                || ratingEngine.getType() != RatingEngineType.CUSTOM_EVENT
                        ? new HashSet<>(Arrays.asList(Category.values()))
                        : CustomEventModelingDataStoreUtil.getCategoriesByDataStores(dataStores);

        stopWatch.resume();
        splitter.start();
        Map<String, ColumnMetadata> iterationAttributes = metadataStoreProxy
                .getMetadata(MetadataStoreName.Table, CustomerSpace.shortenCustomerSpace(customerSpace),
                        table.getName()) //
                .filter(cm -> !Boolean.TRUE.equals(cm.isHiddenForRemodelingUI())) //
                .collectMap(this::getKey).block();
        log.info(
                String.format("Iteration metadata compilation (Split: %d ms Total: %d ms): Retrieve Iteration Metadata",
                        splitter.getTime(), stopWatch.getTime()));
        stopWatch.suspend();
        splitter.reset();
        stopWatch.resume();
        splitter.start();
        List<ColumnMetadata> acctAttrs = servingStoreService.getAttrsEnabledForModeling(customerSpace,
                BusinessEntity.Account, dataCollectionService.getActiveVersion(customerSpace)).collectList().block();
        Map<String, ColumnMetadata> modelingAttributes = Flux.fromIterable(acctAttrs)
                .concatWith(servingStoreService.getAttrsEnabledForModeling(customerSpace,
                        BusinessEntity.AnalyticPurchaseState, dataCollectionService.getActiveVersion(customerSpace)))
                .filter(cm -> selectedCategories.contains(cm.getCategory()))
                .filter(cm -> !Boolean.TRUE.equals(cm.isHiddenForRemodelingUI())) //
                .collectMap(this::getKey, cm -> {
                    ColumnMetadata toReturn = iterationAttributes.getOrDefault(getKey(cm), cm);
                    cm.setSubcategory(Category.SUB_CAT_OTHER);
                    toReturn.setAdminDisabledForModel(false);
                    cm.setAdminDisabledForModel(false);
                    return toReturn;
                }, () -> iterationAttributes).block();
        log.info(String.format(
                "Iteration metadata compilation (Split: %d ms Total: %d ms): Retrieve Modeling metadata from Attr Admin and final compilation",
                splitter.getTime(), stopWatch.getTime()));
        stopWatch.suspend();
        splitter.reset();

        if (MapUtils.isEmpty(modelingAttributes)) {
            throw new LedpException(LedpCode.LEDP_40036,
                    new String[] { "Modeling Attributes", aiModel.getId(), ratingEngine.getId(), customerSpace });
        }
        modelingAttributes.forEach((key, cm) -> {
            if (!Boolean.FALSE.equals(cm.isAdminDisabledForModel())) {
                cm.setApprovedUsageList(Collections.singletonList(ApprovedUsage.NONE));
            }
        });
        populateFeatureImportances(customerSpace, stopWatch, splitter, modelSummary, predictors, modelingAttributes);
        return modelingAttributes;
    }

    private void populateFeatureImportances(String customerSpace, StopWatch stopWatch, StopWatch splitter,
            ModelSummary modelSummary, Map<String, Predictor> predictors,
            Map<String, ColumnMetadata> modelingAttributes) {

        stopWatch.resume();
        splitter.start();
        Map<String, Integer> importanceOrdering = featureImportanceUtil.getFeatureImportance(customerSpace,
                modelSummary);
        log.info(String.format("Iteration metadata compilation (Split: %d ms Total: %d ms): Build Feature Importance",
                splitter.getTime(), stopWatch.getTime()));
        stopWatch.suspend();
        splitter.reset();

        stopWatch.resume();
        splitter.start();
        modelingAttributes.forEach((k, cm) -> {
            if (importanceOrdering.containsKey(cm.getAttrName())) {
                // could move this into le-metadata as a decorator
                cm.setImportanceOrdering(importanceOrdering.get(cm.getAttrName()));
                importanceOrdering.remove(cm.getAttrName());
            }
            cm.setPredictivePower(
                    predictors.getOrDefault(cm.getAttrName(), new Predictor()).getUncertaintyCoefficient());
            cm.setEntity(BusinessEntity.Account);
        });
        log.info(String.format(
                "Iteration metadata compilation: " + "Setting Importance Ordering (Split: %d ms Total: %d ms)",
                splitter.getTime(), stopWatch.getTime()));

        if (MapUtils.isNotEmpty(importanceOrdering)) {
            log.info("AttributesNotFound: " + StringUtils.join(", ", importanceOrdering.keySet()));
        }
    }

    @Override
    public Map<String, StatsCube> getIterationMetadataCube(String customerSpace, RatingEngine ratingEngine,
            AIModel aiModel, List<CustomEventModelingConfig.DataStore> dataStores) {
        List<ColumnMetadata> metadataAttrs = getIterationMetadata(customerSpace, ratingEngine, aiModel, dataStores);
        return getIterationMetadataCube(customerSpace, aiModel.getModelSummaryId(), metadataAttrs);
    }

    public Map<String, StatsCube> getIterationMetadataCube(String customerSpace, String modelSummaryId,
            List<ColumnMetadata> metadataAttrs) {
        ModelSummary modelSummary = modelSummaryProxy.findByModelId(customerSpace, modelSummaryId, true, false, false);
        Map<String, Predictor> predictors = extractPredictorsFromSummary(modelSummary);

        Map<String, AttributeStats> predictorStats = Flux.fromIterable(metadataAttrs)
                .collectMap(ColumnMetadata::getAttrName,
                        cm -> convertToAttributeStats(cm, predictors.getOrDefault(cm.getAttrName(), null)))
                .block();

        StatsCube modelingAttrsStatsCube = new StatsCube();
        modelingAttrsStatsCube.setStatistics(predictorStats);

        return ImmutableMap.of(statsCubeKey, modelingAttrsStatsCube);
    }

    @Override
    public TopNTree getIterationMetadataTopN(String customerSpace, RatingEngine ratingEngine, AIModel aiModel,
            List<CustomEventModelingConfig.DataStore> dataStores) {
        List<ColumnMetadata> metadataAttrs = getIterationMetadata(customerSpace, ratingEngine, aiModel, dataStores);
        Map<String, StatsCube> iterationStatsCube = getIterationMetadataCube(customerSpace, aiModel.getModelSummaryId(),
                metadataAttrs);

        return StatsCubeUtils.constructTopNTreeForIteration(iterationStatsCube,
                ImmutableMap.of(statsCubeKey, metadataAttrs));
    }

    private String getKey(ColumnMetadata cm) {
        return cm.getCategory().getName() + cm.getAttrName();
    }

    private AttributeStats convertToAttributeStats(ColumnMetadata cm, Predictor predictor) {
        AttributeStats attrStat = new AttributeStats();
        attrStat.setBuckets(new Buckets());
        boolean predictorsElementsExist = predictor != null
                && CollectionUtils.isNotEmpty(predictor.getPredictorElements());
        cm.setCanEnrich(!predictorsElementsExist);
        attrStat.setNonNullCount(predictorsElementsExist
                ? predictor.getPredictorElements().stream().mapToLong(PredictorElement::getCount).sum()
                : 0);
        attrStat.getBuckets()
                .setBucketList(predictorsElementsExist
                        ? predictor.getPredictorElements().stream().map(this::convertToBucket)
                                .sorted(Comparator.comparing(Bucket::getCount).reversed()).collect(Collectors.toList())
                        : new ArrayList<>());

        attrStat.getBuckets().setType(predictorsElementsExist ? interpretType(predictor) : BucketType.Enum);

        return attrStat;

    }

    private BucketType interpretType(Predictor predictor) {
        return predictor.getPredictorElements().stream().allMatch(e -> CollectionUtils.isNotEmpty(e.getValuesList())
                && e.getLowerInclusive() == null && e.getUpperExclusive() == null) ? BucketType.Enum
                        : BucketType.Numerical;
    }

    private Bucket convertToBucket(PredictorElement predictorElement) {
        Bucket bkt = new Bucket();
        bkt.setCount(predictorElement.getCount());
        bkt.setLift(predictorElement.getLift());
        if (CollectionUtils.isNotEmpty(
                predictorElement.getValuesList().stream().filter(Objects::nonNull).collect(Collectors.toList()))) {
            bkt.setLabel((StringUtils.isBlank(predictorElement.getValuesList().get(0))
                    || predictorElement.getValuesList().get(0).toLowerCase().equals("null") ? nullLabel
                            : predictorElement.getValuesList().get(0)));
            bkt.setComparisonType(ComparisonType.EQUAL);
            bkt.setValues(Collections.singletonList(bkt.getLabel()));
        } else if (predictorElement.getLowerInclusive() == null && predictorElement.getUpperExclusive() == null) {
            bkt.setLabel(nullLabel);
            bkt.setComparisonType(ComparisonType.EQUAL);
            bkt.setValues(Collections.singletonList(bkt.getLabel()));
        } else if (predictorElement.getLowerInclusive() == null) {
            bkt.setLabel("< " + predictorElement.getUpperExclusive());
            bkt.setComparisonType(ComparisonType.LESS_THAN);
            bkt.setValues(Collections.singletonList(predictorElement.getUpperExclusive()));
        } else if (predictorElement.getUpperExclusive() == null) {
            bkt.setLabel(">= " + predictorElement.getLowerInclusive());
            bkt.setComparisonType(ComparisonType.GREATER_OR_EQUAL);
            bkt.setValues(Collections.singletonList(predictorElement.getLowerInclusive()));
        } else {
            bkt.setLabel(predictorElement.getLowerInclusive() + " - " + predictorElement.getUpperExclusive());
            bkt.setComparisonType(ComparisonType.GTE_AND_LT);
            bkt.setValues(Arrays.asList(predictorElement.getLowerInclusive(), predictorElement.getUpperExclusive()));
        }
        return bkt;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Predictor> extractPredictorsFromSummary(ModelSummary modelSummary) {
        Map<String, Object> maps = JsonUtils.deserialize(modelSummary.getDetails().getPayload(), Map.class);
        return Flux.fromIterable(JsonUtils.convertList(((List<?>) maps.get("Predictors")), Predictor.class))
                .collectMap(Predictor::getName).block();
    }

}
