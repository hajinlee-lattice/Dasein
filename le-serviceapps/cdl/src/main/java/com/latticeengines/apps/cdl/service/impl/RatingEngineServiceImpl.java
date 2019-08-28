package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.apps.cdl.service.impl.RatingModelServiceBase.getRatingModelService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.impl.DependencyChecker;
import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingModelService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.util.CustomEventModelingDataStoreUtil;
import com.latticeengines.apps.cdl.workflow.CrossSellImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.CustomEventModelingWorkflowSubmitter;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.cdl.CrossSellModelingParameters;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelWithPublishedHistoryDTO;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.proxy.exposed.lp.ModelCopyProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Component("ratingEngineService")
public class RatingEngineServiceImpl extends RatingEngineTemplate implements RatingEngineService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineServiceImpl.class);

    @Value("${cdl.rating.service.threadpool.size:20}")
    private Integer fetcherNum;

    @Value("${cdl.rating.crossell.minimum.events:50}")
    private Long minimumEvents;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private SegmentService segmentService;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private EventProxy eventProxy;

    @Inject
    private CrossSellImportMatchAndModelWorkflowSubmitter crossSellImportMatchAndModelWorkflowSubmitter;

    @Inject
    private CustomEventModelingWorkflowSubmitter customEventModelingWorkflowSubmitter;

    @Inject
    private TableRoleTemplate tableRoleTemplate;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ServingStoreCacheService servingStoreCacheService;

    @Inject
    private ModelCopyProxy modelCopyProxy;

    @Inject
    private PlayService playService;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Inject
    private ProxyResourceService proxyResourceService;

    @Inject
    private DependencyChecker dependencyChecker;

    @Value("${cdl.model.delete.propagate:false}")
    private Boolean shouldPropagateDelete;

    private ForkJoinPool tpForParallelStream;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private BatonService batonService;

    @PostConstruct
    public void postConstruct() {
        tpForParallelStream = ThreadPoolUtils.getForkJoinThreadPool("rating-details-fetcher", fetcherNum);
    }

    @Override
    public List<RatingEngine> getAllRatingEngines() {
        Tenant tenant = MultiTenantContext.getTenant();
        List<RatingEngine> result = ratingEngineEntityMgr.findAll();
        updateLastRefreshedDate(tenant.getId(), result);
        return result;
    }

    @Override
    public List<RatingEngineSummary> getAllRatingEngineSummaries() {
        return getAllRatingEngineSummaries(null, null);
    }

    @Override
    public List<RatingEngine> getAllDeletedRatingEngines() {
        return ratingEngineEntityMgr.findAllDeleted();
    }

    @Override
    public List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status) {
        return getAllRatingEngineSummaries(type, status, false);
    }

    @Override
    public List<RatingEngineSummary> getAllRatingEngineSummaries(String type, String status,
            boolean publishedRatingsOnly) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format(
                "Get all the rating engine summaries for tenant %s with status set to %s and type set to %s",
                tenant.getId(), status, type));

        long timestamp = System.currentTimeMillis();
        Date lastRefreshedDate = findLastRefreshedDate(tenant.getId());
        log.info(String.format("Fetched lastRefreshedDate %s in %d ms", lastRefreshedDate,
                (System.currentTimeMillis() - timestamp)));

        List<RatingEngine> list = ratingEngineEntityMgr.findAllByTypeAndStatus(type, status);
        List<RatingEngine> selectedList = list;
        if (publishedRatingsOnly) {
            Set<String> publishedRatingEngineIds = getPublishedRatingEngineIds();
            selectedList = list.stream().filter(ratingEngine -> publishedRatingEngineIds.contains(ratingEngine.getId()))
                    .collect(Collectors.toList());
        }

        final List<RatingEngine> finalSelectedList = selectedList;

        timestamp = System.currentTimeMillis();
        List<RatingEngineSummary> result = constructRatingEngineSummaries(finalSelectedList, tenant.getId(),
                lastRefreshedDate);
        log.info(String.format("Executed constructRatingEngineSummary in %d ms",
                (System.currentTimeMillis() - timestamp)));

        return result;
    }

    @Override
    public List<String> getAllRatingEngineIdsInSegment(String segmentName) {
        return getAllRatingEngineIdsInSegment(segmentName, true);
    }

    @Override
    public List<String> getAllRatingEngineIdsInSegment(String segmentName, //
            boolean considerPublishedOnly) {
        List<String> ids = ratingEngineEntityMgr.findAllIdsInSegment(segmentName);
        if (considerPublishedOnly) {
            ids.retainAll(getPublishedRatingEngineIds());
        }
        return ids;
    }

    @Override
    public Map<String, Long> updateRatingEngineCounts(String engineId) {
        Map<String, Long> counts = null;
        RatingEngine ratingEngine = getRatingEngineById(engineId, false, true);
        if (ratingEngine != null) {
            counts = updateRatingCount(ratingEngine);
            log.info("Updated counts for rating engine " + engineId + " to " + JsonUtils.serialize(counts));
        } else {
            log.warn("Cannot find engine with id " + engineId);
        }
        return counts;
    }

    @Override
    public RatingEngine getRatingEngineById(String ratingEngineId, boolean populateRefreshedDate, boolean inflate) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId, inflate);
        if (populateRefreshedDate) {
            updateLastRefreshedDate(tenant.getId(), ratingEngine);
        }
        return ratingEngine;
    }

    @Override
    public RatingEngine getRatingEngineById(String ratingEngineId, boolean populateRefreshedDate) {
        return getRatingEngineById(ratingEngineId, populateRefreshedDate, false);
    }

    @Override
    public RatingEngine createOrUpdate(RatingEngine ratingEngine) {
        return createOrUpdate(ratingEngine, false);
    }

    @Override
    public RatingEngine createOrUpdate(RatingEngine ratingEngine, Boolean unlinkSegment) {
        if (ratingEngine == null) {
            throw new NullPointerException("Entity is null when creating a rating engine.");
        }

        Tenant tenant = MultiTenantContext.getTenant();
        if (ratingEngine.getSegment() != null && ratingEngine.getSegment().getPid() == null) {
            String segmentName = ratingEngine.getSegment().getName();
            MetadataSegment segment = segmentService.findByName(segmentName);
            ratingEngine.setSegment(segment);
        }

        if (ratingEngine.getId() == null) { // create a new Rating Engine
            ratingEngine.setId(RatingEngine.generateIdStr());
            ratingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngine);
        } else {
            RatingEngine retrievedRatingEngine = ratingEngineEntityMgr.findById(ratingEngine.getId());
            if (retrievedRatingEngine == null) {
                log.warn(String.format("Rating Engine with id %s for tenant %s cannot be found", ratingEngine.getId(),
                        tenant.getId()));
                // ratingEngine.setId(RatingEngine.generateIdStr());
                ratingEngine = ratingEngineEntityMgr.createRatingEngine(ratingEngine);
            } else { // update an existing one by updating the delta passed from
                // front end
                ratingEngine = ratingEngineEntityMgr.updateRatingEngine(ratingEngine, retrievedRatingEngine,
                        unlinkSegment);
                evictRatingMetadataCache();
            }
        }
        updateLastRefreshedDate(tenant.getId(), ratingEngine);
        return ratingEngine;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RatingModel createModelIteration(RatingEngine ratingEngine, RatingModel ratingModel) {
        if (ratingEngine.getType() == RatingEngineType.RULE_BASED || !(ratingModel instanceof AIModel)) {
            throw new LedpException(LedpCode.LEDP_40038,
                    new String[] { ratingEngine.getType().getRatingEngineTypeName() });
        }

        String ratingEngineId = ratingEngine.getId();
        log.info("Creating new iteration for rating engine %s", ratingEngineId);

        RatingModelService<AIModel> ratingModelService = RatingModelServiceBase
                .getRatingModelService(ratingEngine.getType());
        AIModel newIteration = ratingModelService.createNewIteration((AIModel) ratingModel, ratingEngine);

        ratingEngine.setLatestIteration(newIteration);
        createOrUpdate(ratingEngine);
        return newIteration;
    }

    @Override
    public RatingEngine replicateRatingEngine(String id) {
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(id, true);
        if (ratingEngine != null) {
            String ratingEngineId = ratingEngine.getId();
            log.info(String.format("Replicating rating engine %s (%s)", ratingEngineId, ratingEngine.getDisplayName()));

            RatingModel latestIteration = ratingEngine.getLatestIteration();
            RatingEngine copy = new RatingEngine();
            String displayName = ratingEngine.getDisplayName();
            copy.setDisplayName(generateReplicaName(displayName));
            copy.setDescription(ratingEngine.getDescription());
            copy.setType(ratingEngine.getType());
            copy.setSegment(ratingEngine.getSegment());
            copy.setStatus(RatingEngineStatus.INACTIVE);
            copy.setTenant(ratingEngine.getTenant());
            copy.setAdvancedRatingConfig(ratingEngine.getAdvancedRatingConfig());
            copy.setCreatedBy(ratingEngine.getCreatedBy());
            copy.setUpdatedBy(ratingEngine.getUpdatedBy());

            copy = createOrUpdate(copy);
            log.info("Replicated rating engine " + ratingEngineId + " to " + copy.getId());

            if (latestIteration != null) {
                log.info("Replicating latest Iteration " + latestIteration.getId() + " for replicated engine "
                        + copy.getId() + "(" + copy.getDisplayName() + ")");
                RatingModel replicatedModel = replicateRatingModel(copy.getLatestIteration(), latestIteration,
                        copy.getType());
                log.info("Replicated the latest iteration " + replicatedModel.getId() + " in replicated engine "
                        + copy.getId() + "(" + copy.getDisplayName() + ")");
            }
            return copy;
        } else {
            return null;
        }
    }

    private RatingModel replicateRatingModel(RatingModel copy, RatingModel original, RatingEngineType type) {
        if (type == RatingEngineType.RULE_BASED) {
            return replicateRuleBasedModel((RuleBasedModel) copy, (RuleBasedModel) original);
        } else {
            return replicateAIModel((AIModel) copy, (AIModel) original, type);
        }
    }

    @SuppressWarnings("unchecked")
    private AIModel replicateAIModel(AIModel copy, AIModel original, RatingEngineType type) {
        copy.setAdvancedModelingConfig(original.getAdvancedModelingConfig());
        copy.setModelingJobStatus(original.getModelingJobStatus());
        copy.setPredictionType(original.getPredictionType());
        copy.setTrainingSegment(original.getTrainingSegment());
        copy.setRatingModelAttributes(original.getRatingModelAttributes());
        if (StringUtils.isNotBlank(original.getModelSummaryId())) {
            String tenantId = MultiTenantContext.getTenant().getId();
            String replicatedModelGUID = modelCopyProxy.copyModel(tenantId, tenantId, original.getModelSummaryId(),
                    "false");
            modelSummaryProxy.setDownloadFlag(tenantId);
            copy.setModelSummaryId(replicatedModelGUID);
        }
        RatingModelService<AIModel> ratingModelService = RatingModelServiceBase.getRatingModelService(type);
        return ratingModelService.createOrUpdate(copy, copy.getRatingEngine().getId());
    }

    @SuppressWarnings("unchecked")
    private RuleBasedModel replicateRuleBasedModel(RuleBasedModel copy, RuleBasedModel original) {
        copy.setRatingRule(original.getRatingRule());
        copy.setSelectedAttributes(original.getSelectedAttributes());
        copy.setRatingModelAttributes(original.getRatingModelAttributes());

        RatingModelService<RuleBasedModel> ratingModelService = RatingModelServiceBase
                .getRatingModelService(RatingEngineType.RULE_BASED);
        return ratingModelService.createOrUpdate(copy, copy.getRatingEngine().getId());
    }

    @Override
    public void deleteById(String id) {
        deleteById(id, true, null);
    }

    @Override
    public void deleteById(String id, boolean hardDelete, String actionInitiator) {
        checkFeasibilityForDelete(id);
        if (shouldPropagateDelete == Boolean.TRUE) {
            List<Play> relatedPlays = playService.getAllFullPlays(false, id);
            if (CollectionUtils.isNotEmpty(relatedPlays)) {
                relatedPlays.forEach(p -> playService.deleteByName(p.getName(), hardDelete));
            }
        }
        attrConfigEntityMgr.deleteByAttrNameStartingWith(id);
        ratingEngineEntityMgr.deleteById(id, hardDelete, actionInitiator);
        evictRatingMetadataCache();
    }

    private void checkFeasibilityForDelete(String id) {
        if (StringUtils.isBlank(id)) {
            throw new NullPointerException("RatingEngine id cannot be empty");
        }
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(id);
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            throw new NullPointerException("RatingEngine cannot be found");
        }

        if (ratingEngine.getStatus() != null && ratingEngine.getStatus() != RatingEngineStatus.INACTIVE) {
            throw new LedpException(LedpCode.LEDP_18181, new String[] { ratingEngine.getDisplayName() });
        }
    }

    @Override
    public void revertDelete(String id) {
        ratingEngineEntityMgr.revertDelete(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false);
        RatingModelService<RatingModel> ratingModelService = getRatingModelService(ratingEngine.getType());
        return ratingModelService.getAllRatingModelsByRatingEngineId(ratingEngineId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RatingModel getRatingModel(String ratingEngineId, String ratingModelId) {
        RatingEngine ratingEngine = validateRatingEngine(ratingEngineId);
        RatingModelService<RatingModel> ratingModelService = getRatingModelService(ratingEngine.getType());
        return ratingModelService.getRatingModelById(ratingModelId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RatingModel updateRatingModel(String ratingEngineId, String ratingModelId, RatingModel ratingModel) {
        log.info(String.format("Update ratingModel %s for Rating Engine %s", ratingModelId, ratingEngineId));
        RatingEngine ratingEngine = validateRatingEngine(ratingEngineId);
        RatingModelService<RatingModel> ratingModelService = getRatingModelService(ratingEngine.getType());
        ratingModel.setId(ratingModelId);
        RatingModel model = ratingModelService.createOrUpdate(ratingModel, ratingEngineId);
        evictRatingMetadataCache();
        return model;
    }

    @Override
    public List<ColumnMetadata> getIterationMetadata(String customerSpace, String ratingEngineId, String ratingModelId,
            List<CustomEventModelingConfig.DataStore> dataStores) {
        log.info(String.format(ratingModelId, "Attempting to collate Metadata for Iteration %s of Model %s",
                ratingEngineId));
        customerSpace = MultiTenantContext.getCustomerSpace().toString();
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        validateAIRatingEngine(ratingEngine);
        AIModel aiModel = (AIModel) getRatingModel(ratingEngineId, ratingModelId);
        AIModelService aiModelService = (AIModelService) getRatingModelService(ratingEngine.getType());

        return aiModelService.getIterationMetadata(customerSpace, ratingEngine, aiModel, dataStores);
    }

    @Override
    public Map<String, List<ColumnMetadata>> getIterationAttributes(String customerSpace, String ratingEngineId,
            String ratingModelId, List<CustomEventModelingConfig.DataStore> dataStores) {
        log.info(String.format("Attempting to collate Metadata for Iteration %s of Model %s", ratingModelId,
                ratingEngineId));
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        validateAIRatingEngine(ratingEngine);
        AIModel aiModel = (AIModel) getRatingModel(ratingEngineId, ratingModelId);
        AIModelService aiModelService = (AIModelService) getRatingModelService(ratingEngine.getType());

        return aiModelService.getIterationAttributes(customerSpace, ratingEngine, aiModel, dataStores);
    }

    @Override
    public Map<String, StatsCube> getIterationMetadataCube(String customerSpace, String ratingEngineId,
            String ratingModelId, List<CustomEventModelingConfig.DataStore> dataStores) {
        log.info(String.format("Attempting to generate StasCube for Iteration %s of Model %s", ratingModelId,
                ratingEngineId));
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        validateAIRatingEngine(ratingEngine);
        AIModel aiModel = (AIModel) getRatingModel(ratingEngineId, ratingModelId);
        AIModelService aiModelService = (AIModelService) getRatingModelService(ratingEngine.getType());

        return aiModelService.getIterationMetadataCube(customerSpace, ratingEngine, aiModel, dataStores);
    }

    @Override
    public TopNTree getIterationMetadataTopN(String customerSpace, String ratingEngineId, String ratingModelId,
            List<CustomEventModelingConfig.DataStore> dataStores) {
        log.info(String.format("Attempting to generate StasCube for Iteration %s of Model %s", ratingModelId,
                ratingEngineId));
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        validateAIRatingEngine(ratingEngine);
        AIModel aiModel = (AIModel) getRatingModel(ratingEngineId, ratingModelId);
        AIModelService aiModelService = (AIModelService) getRatingModelService(ratingEngine.getType());

        return aiModelService.getIterationMetadataTopN(customerSpace, ratingEngine, aiModel, dataStores);
    }

    @Override
    public void setScoringIteration(String customerSpace, String ratingEngineId, String ratingModelId, List<BucketMetadata> bucketMetadatas,
            String userEmail) {

        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, true);
        RatingModel ratingModel = getRatingModel(ratingEngineId, ratingModelId);

        if (ratingEngine.getType() != RatingEngineType.RULE_BASED) {
            if (bucketMetadatas == null || CollectionUtils.isEmpty(bucketMetadatas)) {
                throw new LedpException(LedpCode.LEDP_40030, new String[] { ratingModelId, ratingEngineId });
            }
            AIModel aiModel = (AIModel) ratingModel;
            if (StringUtils.isEmpty(aiModel.getModelSummaryId())) {
                throw new LedpException(LedpCode.LEDP_40031, new String[] { ratingModelId, ratingEngineId });
            }

            log.info(String.format("Creating BucketMetadata for RatingEngine %s, AIModel %s and ModelSummary %s",
                    ratingEngineId, ratingModelId, aiModel.getModelSummaryId()));

            if (!bucketMetadatas.equals(ratingEngine.getBucketMetadata())) {
                CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
                request.setBucketMetadataList(bucketMetadatas);
                request.setModelGuid(aiModel.getModelSummaryId());
                request.setRatingEngineId(ratingEngineId);
                request.setLastModifiedBy(userEmail);
                boolean targetScoreDerivation = batonService.isEnabled(CustomerSpace.parse(customerSpace), LatticeFeatureFlag.ENABLE_TARGET_SCORE_DERIVATION);
                if (targetScoreDerivation) {
                    request.setCreateForModel(true);
                }
                bucketedScoreProxy.createABCDBuckets(MultiTenantContext.getShortTenantId(), request);
            }
        }

        ratingEngine.setScoringIteration(ratingModel);
        createOrUpdate(ratingEngine);
    }

    @Override
    public Map<String, List<String>> getRatingEngineDependencies(String customerSpace, String ratingEngineId) {
        log.info(String.format("Attempting to find rating engine dependencies for Rating Engine %s", ratingEngineId));

        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        if (ratingEngine == null) {
            throw new LedpException(LedpCode.LEDP_40016, new String[] { ratingEngineId, customerSpace });
        }

        HashMap<String, List<String>> dependencyMap = new HashMap<>();
        try {
            Map<String, List<String>> dep = dependencyChecker.getDependencies(customerSpace, ratingEngineId,
                    CDLObjectTypes.Model.name());
            if (MapUtils.isNotEmpty(dep)) {
                dependencyMap.putAll(dep);
            }
        } catch (Exception e) {
            log.info("fallback to original logic till graph based lookup is not stable: " + e.getMessage(), e);

            dependencyMap.put(CDLObjectTypes.Play.name(), playEntityMgr.findAllByRatingEnginePid(ratingEngine.getPid())
                    .stream().map(Play::getDisplayName).collect(Collectors.toList()));
        }
        return dependencyMap;
    }

    @Override
    public EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version) {
        if (ratingModel == null) {
            throw new LedpException(LedpCode.LEDP_40014, new String[] { ratingEngine.getId(), customerSpace });
        }

        if (ratingEngine.getType() == RatingEngineType.CROSS_SELL && ratingModel instanceof AIModel) {
            AIModelService aiModelService = (AIModelService) getRatingModelService(ratingEngine.getType());
            if (version == null) {
                version = dataCollectionService.getActiveVersion(customerSpace);
            }
            return aiModelService.getModelingQuery(customerSpace, ratingEngine, (AIModel) ratingModel,
                    modelingQueryType, version);
        } else {
            throw new LedpException(LedpCode.LEDP_40009,
                    new String[] { ratingEngine.getId(), ratingModel.getId(), customerSpace });
        }
    }

    @Override
    public Long getModelingQueryCount(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version) {
        EventFrontEndQuery efeq = getModelingQuery(customerSpace, ratingEngine, ratingModel, modelingQueryType,
                version);
        log.info("getModelingQueryCount ratingEngine: " + JsonUtils.serialize(ratingEngine));
        log.info("getModelingQueryCount ratingModel: " + JsonUtils.serialize(ratingModel));
        log.info("getModelingQueryCount efeq: " + JsonUtils.serialize(efeq));
        switch (modelingQueryType) {
        case TARGET:
            return eventProxy.getScoringCount(customerSpace, efeq, version);
        case TRAINING:
            return eventProxy.getTrainingCount(customerSpace, efeq, version);
        case EVENT:
            return eventProxy.getEventCount(customerSpace, efeq, version);
        default:
            throw new LedpException(LedpCode.LEDP_40010, new String[] { modelingQueryType.getModelingQueryTypeName() });
        }
    }

    @Override
    public boolean validateForModeling(String customerSpace, RatingEngine ratingEngine, AIModel aiModel) {
        validateAIRatingEngine(ratingEngine);
        List<String> errors = new ArrayList<>();
        switch (ratingEngine.getType()) {
        case RULE_BASED:
            errors.add(LedpException.buildMessage(LedpCode.LEDP_31107,
                    new String[] { RatingEngineType.RULE_BASED.getRatingEngineTypeName() }));
            break;
        case CROSS_SELL:
            if (CollectionUtils
                    .isEmpty(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts())) {
                errors.add(LedpException.buildMessage(LedpCode.LEDP_40012,
                        new String[] { aiModel.getId(), CustomerSpace.parse(customerSpace).toString() }));
            }
            Long noOfEvents = getModelingQueryCount(customerSpace, ratingEngine, aiModel, ModelingQueryType.EVENT,
                    null);
            if (noOfEvents < minimumEvents) {
                errors.add(LedpException.buildMessage(LedpCode.LEDP_40046, new String[] { minimumEvents.toString() }));
            }
            break;
        case CUSTOM_EVENT:
            List<CustomEventModelingConfig.DataStore> dataStores = CustomEventModelingConfig
                    .getAdvancedModelingConfig(aiModel).getDataStores();
            if (CustomEventModelingConfig.getAdvancedModelingConfig(aiModel).getCustomEventModelingType() == null) {
                errors.add("No CustomEventModelingType selected");
            }
            if (CustomEventModelingConfig.getAdvancedModelingConfig(aiModel)
                    .getCustomEventModelingType() == CustomEventModelingType.CDL && ratingEngine.getSegment() == null) {
                errors.add("No Target segment selected for the model");
            }
            if (CollectionUtils.isEmpty(dataStores)) {
                errors.add("No datastore selected, atleast one attribute set needed for modeling");
            }

            if (aiModel.getIteration() == 1) {
                Set<Category> selectedCategories = CustomEventModelingDataStoreUtil
                        .getCategoriesByDataStores(dataStores);
                Flux<ColumnMetadata> flux = proxyResourceService.getNewModelingAttrs(customerSpace,
                        BusinessEntity.Account, null);
                List<ColumnMetadata> userSelectedAttributesForModeling = flux.filter(cm -> selectedCategories.contains(cm.getCategory()))
                        .collectList().block();
                if (CollectionUtils.isEmpty(userSelectedAttributesForModeling)) {
                    errors.add(LedpCode.LEDP_40044.getMessage());
                }
            }
            break;
        case PROSPECTING:
            break;
        default:
            break;
        }
        if (CollectionUtils.isEmpty(errors)) {
            return true;
        } else {
            AtomicInteger i = new AtomicInteger(1);
            throw new LedpException(LedpCode.LEDP_32000,
                    errors.stream().map(err -> "\n" + i.getAndIncrement() + ". " + err).toArray());
        }
    }

    @Override
    public String modelRatingEngine(String customerSpace, RatingEngine ratingEngine, AIModel aiModel,
            List<ColumnMetadata> userRefinedColumnMetadata, String userEmail) {
        validateForModeling(customerSpace, ratingEngine, aiModel);
        ApplicationId jobId = aiModel.getModelingYarnJobId();
        if (jobId != null) {
            return jobId.toString();
        }

        if (CollectionUtils.isEmpty(userRefinedColumnMetadata) && aiModel.getDerivedFromRatingModel() != null) {
            userRefinedColumnMetadata = getIterationMetadata(customerSpace, ratingEngine.getId(),
                    aiModel.getDerivedFromRatingModel(), null);
        }

        Map<String, ColumnMetadata> userRefinedAttributes = null;

        if (CollectionUtils.isNotEmpty(userRefinedColumnMetadata)) {
            userRefinedAttributes = Flux.fromIterable(userRefinedColumnMetadata) //
                    .collectMap(ColumnMetadata::getAttrName) //
                    .block();
        }

        switch (ratingEngine.getType()) {
        case RULE_BASED:
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { RatingEngineType.RULE_BASED.getRatingEngineTypeName() });
        case CROSS_SELL:
            modelSummaryProxy.setDownloadFlag(CustomerSpace.parse(customerSpace).toString());
            DataCollection.Version activeVersion = dataCollectionService.getActiveVersion(customerSpace);
            CrossSellModelingParameters crossSellModelingParameters = new CrossSellModelingParameters();
            crossSellModelingParameters.setName(aiModel.getId());
            crossSellModelingParameters.setDisplayName(ratingEngine.getDisplayName() + "_" + aiModel.getIteration());
            crossSellModelingParameters.setDescription(ratingEngine.getDisplayName());
            crossSellModelingParameters.setModuleName("Module");
            crossSellModelingParameters.setUserId(userEmail);
            crossSellModelingParameters.setRatingEngineId(ratingEngine.getId());
            crossSellModelingParameters.setAiModelId(aiModel.getId());
            crossSellModelingParameters.setTargetFilterQuery(
                    getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.TARGET, activeVersion));
            crossSellModelingParameters.setTargetFilterTableName(aiModel.getId() + "_target");
            crossSellModelingParameters.setTrainFilterQuery(
                    getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.TRAINING, activeVersion));
            crossSellModelingParameters.setTrainFilterTableName(aiModel.getId() + "_train");
            crossSellModelingParameters.setEventFilterQuery(
                    getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.EVENT, activeVersion));
            crossSellModelingParameters.setEventFilterTableName(aiModel.getId() + "_event");
            crossSellModelingParameters.setUserRefinedAttributes(userRefinedAttributes);
            crossSellModelingParameters.setModelIteration(aiModel.getIteration());
            crossSellModelingParameters.setDataCloudVersion(aiModel.getAdvancedModelingConfig().getDataCloudVersion());

            if (aiModel.getPredictionType() == PredictionType.EXPECTED_VALUE) {
                crossSellModelingParameters.setExpectedValue(true);
            }

            log.info(String.format("Cross-sell modelling job submitted with crossSellModelingParameters %s",
                    crossSellModelingParameters.toString()));
            jobId = crossSellImportMatchAndModelWorkflowSubmitter.submit(crossSellModelingParameters);
            break;
        case CUSTOM_EVENT:
            CustomEventModelingConfig config = (CustomEventModelingConfig) aiModel.getAdvancedModelingConfig();
            ModelingParameters customEventModelingParameters = new ModelingParameters();
            customEventModelingParameters.setName(aiModel.getId());
            customEventModelingParameters.setDisplayName(ratingEngine.getDisplayName() + "_" + aiModel.getIteration());
            customEventModelingParameters.setDescription(ratingEngine.getDisplayName());
            customEventModelingParameters.setModuleName("Module");
            customEventModelingParameters.setUserId(userEmail);
            customEventModelingParameters.setRatingEngineId(ratingEngine.getId());
            customEventModelingParameters.setAiModelId(aiModel.getId());
            customEventModelingParameters.setCustomEventModelingType(config.getCustomEventModelingType());
            customEventModelingParameters.setFilename(config.getSourceFileName());
            customEventModelingParameters.setActivateModelSummaryByDefault(true);
            customEventModelingParameters.setDeduplicationType(config.getDeduplicationType());
            customEventModelingParameters.setExcludePublicDomains(config.isExcludePublicDomains());
            customEventModelingParameters.setTransformationGroup(config.getConvertedTransformationGroup());
            customEventModelingParameters.setExcludePropDataColumns(
                    !config.getDataStores().contains(CustomEventModelingConfig.DataStore.DataCloud));
            customEventModelingParameters
                    .setExcludeCDLAttributes(!config.getDataStores().contains(CustomEventModelingConfig.DataStore.CDL));
            customEventModelingParameters.setExcludeCustomFileAttributes(
                    !config.getDataStores().contains(CustomEventModelingConfig.DataStore.CustomFileAttributes));
            customEventModelingParameters.setUserRefinedAttributes(userRefinedAttributes);
            customEventModelingParameters.setModelIteration(aiModel.getIteration());
            customEventModelingParameters
                    .setDataCloudVersion(aiModel.getAdvancedModelingConfig().getDataCloudVersion());

            modelSummaryProxy.setDownloadFlag(CustomerSpace.parse(customerSpace).toString());

            log.debug(String.format("Custom event modelling job submitted with crossSellModelingParameters %s",
                    customEventModelingParameters.toString()));
            jobId = customEventModelingWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace).toString(),
                    customEventModelingParameters);
            break;
        default:
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { ratingEngine.getType().getRatingEngineTypeName() });
        }

        aiModel.setModelingJobId(jobId.toString());
        aiModel.setModelingJobStatus(JobStatus.RUNNING);
        updateRatingModel(ratingEngine.getId(), aiModel.getId(), aiModel);
        return jobId.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RatingModelWithPublishedHistoryDTO> getPublishedHistory(String customerSpace, String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false);
        validateAIRatingEngine(ratingEngine);

        List<BucketMetadata> allBuckets = bucketedScoreProxy
                .getAllBucketsByEngineId(CustomerSpace.shortenCustomerSpace(customerSpace), ratingEngineId);

        if (CollectionUtils.isEmpty(allBuckets)) {
            log.warn("No Buckets found for Model: " + ratingEngineId + ", CustomerSpace: " + customerSpace);
            return new ArrayList<>();
        }

        Map<String, AIModel> modelMap = ((Flux<AIModel>) Flux.fromIterable(
                getRatingModelService(ratingEngine.getType()).getAllRatingModelsByRatingEngineId(ratingEngineId)))
                        .filter(rm -> StringUtils.isNotEmpty(rm.getModelSummaryId()))
                        .collect(HashMap<String, AIModel>::new,
                                (returnMap, am) -> returnMap.put(am.getModelSummaryId(), am))
                        .block();

        Map<Long, RatingModelWithPublishedHistoryDTO> bucketsGroupedByTime = Flux.fromIterable(allBuckets)
                .collect(HashMap<Long, RatingModelWithPublishedHistoryDTO>::new, (returnMap, bucket) -> {
                    if (!returnMap.containsKey(bucket.getCreationTimestamp())) {
                        AIModel model = modelMap.get(bucket.getModelSummaryId());
                        returnMap.put(bucket.getCreationTimestamp(), new RatingModelWithPublishedHistoryDTO(model,
                                bucket.getCreationTimestamp(), new ArrayList<>(), bucket.getLastModifiedByUser()));
                    }
                    returnMap.get(bucket.getCreationTimestamp()).getPublishedMetadata().add(bucket);
                }).block();

        return new ArrayList(bucketsGroupedByTime.values());
    }

    @VisibleForTesting
    Set<String> getPublishedRatingEngineIds() {
        Set<String> engineIds = new HashSet<>();
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version version = dataCollectionService.getActiveVersion(customerSpace);
        List<String> tableNames = dataCollectionService.getTableNames(customerSpace, "",
                TableRoleInCollection.PivotedRating, version);
        if (CollectionUtils.isNotEmpty(tableNames)) {
            ParallelFlux<ColumnMetadata> cms = tableRoleTemplate.getUnorderedSchema(TableRoleInCollection.PivotedRating,
                    version);
            if (cms != null) {
                List<String> idList = cms.filter(cm -> cm.getAttrName().startsWith("engine_")) //
                        .map(ColumnMetadata::getAttrName).sequential().collectList().block();
                if (CollectionUtils.isNotEmpty(idList)) {
                    engineIds = new HashSet<>(idList);
                }
            }
        }
        return engineIds;
    }

    private Map<String, Long> updateRatingCount(RatingEngine ratingEngine) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Cannot find a Tenant in MultiTenantContext, skip getting rating count.");
            return Collections.emptyMap();
        } else {
            RatingEngineFrontEndQuery frontEndQuery = new RatingEngineFrontEndQuery();
            frontEndQuery.setRatingEngineId(ratingEngine.getId());
            frontEndQuery.setMainEntity(BusinessEntity.Account);
            Map<String, Long> counts = entityProxy.getRatingCount(tenant.getId(), frontEndQuery);
            log.info("Updating rating engine " + ratingEngine.getId() + " counts " + JsonUtils.serialize(counts));
            ratingEngine.setCountsByMap(counts);
            createOrUpdate(ratingEngine);
            return counts;
        }
    }

    private void updateLastRefreshedDate(String tenantId, List<RatingEngine> ratingEngines) {
        if (CollectionUtils.isNotEmpty(ratingEngines)) {
            Date lastRefreshedDate = findLastRefreshedDate(tenantId);
            ratingEngines.forEach(re -> re.setLastRefreshedDate(lastRefreshedDate));
        }
    }

    private void updateLastRefreshedDate(String tenantId, RatingEngine ratingEngine) {
        if (ratingEngine != null) {
            Date lastRefreshedDate = findLastRefreshedDate(tenantId);
            ratingEngine.setLastRefreshedDate(lastRefreshedDate);
        }
    }

    private RatingEngine validateRatingEngine(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false);
        validateRatingEngine(ratingEngineId, ratingEngine);

        return ratingEngine;
    }

    private RatingEngine validateRatingEngine_PopulateActiveModel(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, true);
        validateRatingEngine(ratingEngineId, ratingEngine);

        return ratingEngine;
    }

    private void validateRatingEngine(String ratingEngineId, RatingEngine ratingEngine) {
        if (ratingEngine == null) {
            throw new NullPointerException(String.format("Rating Engine with id %s is null", ratingEngineId));
        }

        RatingEngineType type = ratingEngine.getType();
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_18154, new String[] { ratingEngineId });
        }
    }

    private void evictRatingMetadataCache() {
        String tenantId = MultiTenantContext.getShortTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        String keyPrefix = tenantId + "|" + BusinessEntity.Rating.name();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.DataLakeCMCache);
        servingStoreCacheService.clearCache(tenantId, BusinessEntity.Rating);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AttributeLookup> getDependentAttrsInAllModels(String ratingEngineId) {
        Set<AttributeLookup> attributes = new HashSet<>();
        RatingEngine ratingEngine = validateRatingEngine(ratingEngineId);
        RatingModelService<RatingModel> ratingModelService = getRatingModelService(ratingEngine.getType());

        List<RatingModel> ratingModels = ratingModelService.getAllRatingModelsByRatingEngineId(ratingEngineId);
        if (ratingModels != null) {
            for (RatingModel ratingModel : ratingModels) {
                ratingModelService.findRatingModelAttributeLookups(ratingModel);
                attributes.addAll(ratingModel.getRatingModelAttributes());
            }
        }

        return new ArrayList<>(attributes);
    }

    @Override
    public List<AttributeLookup> getDependentAttrsInActiveModel(String ratingEngineId) {
        RatingEngine ratingEngine = validateRatingEngine_PopulateActiveModel(ratingEngineId);
        RatingModel latestIteration = ratingEngine.getLatestIteration();
        if (latestIteration != null) {
            return getDependingAttrsInModel(ratingEngine.getType(), ratingEngine.getLatestIteration().getId());
        } else {
            return new ArrayList<>();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AttributeLookup> getDependingAttrsInModel(RatingEngineType engineType, String modelId) {
        Set<AttributeLookup> attributes = new HashSet<>();
        RatingModelService<RatingModel> ratingModelService = getRatingModelService(engineType);
        RatingModel ratingModel = ratingModelService.getRatingModelById(modelId);
        if (ratingModel != null) {
            ratingModelService.findRatingModelAttributeLookups(ratingModel);
            Set<AttributeLookup> attributeLookups = ratingModel.getRatingModelAttributes();
            if (attributeLookups != null) {
                attributes.addAll(attributeLookups);
            }
        }
        return new ArrayList<>(attributes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RatingModel> getDependingRatingModels(List<String> attributes) {
        Set<RatingModel> ratingModelSet = new HashSet<>();
        RatingModelService<RatingModel> ratingModelService;
        List<RatingEngine> ratingEngines = getAllRatingEngines();
        if (ratingEngines != null) {
            ratingEngines = ratingEngines.stream().filter(rating -> rating.getSegment() != null)
                    .collect(Collectors.toList());
            for (RatingEngine ratingEngine : ratingEngines) {
                // TODO: [YSong-M22] this check can be removed
                // TODO: [YSong-M22] after we clean up orphan rating engines
                // TODO: [YSong-M22] (engines without segment)
                if (!Boolean.TRUE.equals(ratingEngine.getDeleted())) {
                    ratingModelService = getRatingModelService(ratingEngine.getType());
                    List<RatingModel> ratingModels = ratingModelService
                            .getAllRatingModelsByRatingEngineId(ratingEngine.getId());
                    if (ratingModels != null) {
                        for (RatingModel ratingModel : ratingModels) {
                            ratingModelService.findRatingModelAttributeLookups(ratingModel);
                            Set<AttributeLookup> attributeLookups = ratingModel.getRatingModelAttributes();
                            if (attributeLookups != null) {
                                for (AttributeLookup modelAttribute : attributeLookups) {
                                    if (attributes.contains(sanitize(modelAttribute.toString()))) {
                                        ratingModelSet.add(ratingModel);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return new ArrayList<>(ratingModelSet);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RatingEngine> getDependingRatingEngines(List<String> attributes) {
        Set<RatingEngine> ratingEngineSet = new HashSet<>();
        if (attributes != null) {
            RatingModelService<RatingModel> ratingModelService;
            List<RatingEngine> ratingEngines = getAllRatingEngines();
            if (ratingEngines != null) {
                ratingEngines = ratingEngines.stream().filter(rating -> rating.getSegment() != null)
                        .collect(Collectors.toList());
                for (RatingEngine ratingEngine : ratingEngines) {
                    // TODO: [YSong-M22] this check can be removed
                    // TODO: [YSong-M22] after we clean up orphan rating engines
                    // TODO: [YSong-M22] (engines without segment)
                    if (!Boolean.TRUE.equals(ratingEngine.getDeleted())) {
                        ratingModelService = getRatingModelService(ratingEngine.getType());
                        List<RatingModel> ratingModels = ratingModelService
                                .getAllRatingModelsByRatingEngineId(ratingEngine.getId());
                        if (CollectionUtils.isNotEmpty(ratingModels)) {
                            rm: for (RatingModel ratingModel : ratingModels) {
                                ratingModelService.findRatingModelAttributeLookups(ratingModel);
                                Set<AttributeLookup> attributeLookups = ratingModel.getRatingModelAttributes();
                                if (attributeLookups != null) {
                                    for (AttributeLookup modelAttribute : attributeLookups) {
                                        if (attributes.contains(sanitize(modelAttribute.toString()))) {
                                            ratingEngineSet.add(ratingEngine);
                                            break rm;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return new ArrayList<>(ratingEngineSet);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RatingModel> getAllRatingModels() {
        List<RatingModel> models = new ArrayList<>();
        RatingModelService<RatingModel> ratingModelService;
        List<RatingEngine> ratingEngines = getAllRatingEngines();
        if (ratingEngines != null) {
            ratingEngines = ratingEngines.stream().filter(rating -> rating.getSegment() != null)
                    .collect(Collectors.toList());
            for (RatingEngine ratingEngine : ratingEngines) {
                if (!Boolean.TRUE.equals(ratingEngine.getDeleted())) {
                    ratingModelService = getRatingModelService(ratingEngine.getType());
                    List<RatingModel> ratingModels = ratingModelService
                            .getAllRatingModelsByRatingEngineId(ratingEngine.getId());
                    if (ratingModels != null) {
                        for (RatingModel ratingModel : ratingModels) {
                            ratingModelService.findRatingModelAttributeLookups(ratingModel);
                        }
                        models.addAll(ratingModels);
                    }
                }
            }
        }

        return models;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void updateModelingJobStatus(String ratingEngineId, String aiModelId, JobStatus newStatus) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false);
        RatingModelService<AIModel> aiModelService = getRatingModelService(ratingEngine.getType());
        ((AIModelService) aiModelService).updateModelingJobStatus(ratingEngineId, aiModelId, newStatus);
    }

    @SuppressWarnings({ "unchecked", "unused" })
    private Map<Long, String> ratingEngineCyclicDependency(RatingEngine ratingEngine, LinkedHashMap<Long, String> map) {
        LinkedHashMap<Long, String> ratingEngineMap = (LinkedHashMap<Long, String>) map.clone();
        ratingEngineMap.put(ratingEngine.getPid(), ratingEngine.getDisplayName());

        if (ratingEngine.getSegment() != null) {
            List<AttributeLookup> attributeLookups = segmentService
                    .findDependingAttributes(Collections.singletonList(ratingEngine.getSegment()));

            for (AttributeLookup attributeLookup : attributeLookups) {
                if (attributeLookup.getEntity() == BusinessEntity.Rating) {
                    RatingEngine childRatingEngine = getRatingEngineById(
                            RatingEngine.toEngineId(attributeLookup.getAttribute()), false);

                    if (childRatingEngine != null) {
                        if (ratingEngineMap.containsKey(childRatingEngine.getPid())) {
                            ratingEngineMap.put(-1L, childRatingEngine.getDisplayName());
                            return ratingEngineMap;
                        } else {
                            return ratingEngineCyclicDependency(childRatingEngine, ratingEngineMap);
                        }
                    }
                }
            }
        }

        return null;
    }

    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }

    public static String generateReplicaName(String name) {
        final String replica = "Replica";
        Pattern pattern = Pattern.compile("^" + replica + "-(\\d+) ");
        Matcher matcher = pattern.matcher(name);
        String newPrefix = replica + "-" + System.currentTimeMillis() + " ";

        String replicaName;
        if (matcher.find()) {
            String oldPrefix = matcher.group(0);
            replicaName = name.replaceFirst(oldPrefix, newPrefix);
        } else {
            replicaName = newPrefix + name;
        }

        return replicaName;
    }

    @VisibleForTesting
    void setTpForParallelStream(ForkJoinPool tpForParallelStream) {
        this.tpForParallelStream = tpForParallelStream;
    }
}
