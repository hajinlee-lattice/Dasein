package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.mds.TableRoleTemplate;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingModelService;
import com.latticeengines.apps.cdl.workflow.CustomEventModelingWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.RatingEngineImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.cdl.RatingEngineDependencyType;
import com.latticeengines.domain.exposed.cdl.RatingEngineModelingParameters;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreCacheService;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

import reactor.core.publisher.ParallelFlux;

@Component("ratingEngineService")
public class RatingEngineServiceImpl extends RatingEngineTemplate implements RatingEngineService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineServiceImpl.class);

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private EventProxy eventProxy;

    @Inject
    private RatingEngineImportMatchAndModelWorkflowSubmitter ratingEngineImportMatchAndModelWorkflowSubmitter;

    @Inject
    private CustomEventModelingWorkflowSubmitter customEventModelingWorkflowSubmitter;

    @Inject
    private TableRoleTemplate tableRoleTemplate;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ServingStoreCacheService servingStoreCacheService;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void postConstruct() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
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
        List<RatingEngine> list = ratingEngineEntityMgr.findAllByTypeAndStatus(type, status);
        List<RatingEngine> selectedList = list;
        if (publishedRatingsOnly) {
            Set<String> availableRatingIdInRedshift = engineIdsAvailableInRedshift();
            log.info(String.format("Available Rating Ids in Redshift are %s", availableRatingIdInRedshift));
            selectedList = list.stream()
                    .filter(ratingEngine -> availableRatingIdInRedshift.contains(ratingEngine.getId()))
                    .collect(Collectors.toList());
        }
        return selectedList.stream().map(ratingEngine -> constructRatingEngineSummary(ratingEngine, tenant.getId()))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllRatingEngineIdsInSegment(String segmentName) {
        List<String> ids = ratingEngineEntityMgr.findAllIdsInSegment(segmentName);
        ids.retainAll(engineIdsAvailableInRedshift());
        return ids;
    }

    @Override
    public Map<String, Long> updateRatingEngineCounts(String engineId) {
        Map<String, Long> counts = null;
        RatingEngine ratingEngine = getRatingEngineById(engineId, false, true);
        if (ratingEngine != null) {
            counts = updateRatingCount(ratingEngine);
            log.info("Updated counts for rating engine " + engineId + " to " + JsonUtils.serialize(counts));
        }
        return counts;
    }

    @Override
    public RatingEngine getRatingEngineById(String ratingEngineId, boolean populateRefreshedDate,
            boolean populateActiveModel) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngine ratingEngine;
        if (populateActiveModel) {
            ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId, true);
        } else {
            ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        }
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
    public RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId) {
        return createOrUpdate(ratingEngine, tenantId, false);
    }

    @Override
    public RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId, Boolean unlinkSegment) {
        if (ratingEngine == null) {
            throw new NullPointerException("Entity is null when creating a rating engine.");
        }
        Tenant tenant = MultiTenantContext.getTenant();
        if (ratingEngine.getSegment() != null) {
            String segmentName = ratingEngine.getSegment().getName();
            MetadataSegmentDTO segmentDTO = segmentProxy.getMetadataSegmentWithPidByName(tenantId, segmentName);
            MetadataSegment segment = segmentDTO.getMetadataSegment();
            segment.setPid(segmentDTO.getPrimaryKey());
            ratingEngine.setSegment(segment);
        }

        ratingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine, tenantId, unlinkSegment);
        updateLastRefreshedDate(tenant.getId(), ratingEngine);
        evictRatingMetadataCache();
        return ratingEngine;
    }

    @Override
    public void deleteById(String id) {
        deleteById(id, true);
    }

    @Override
    public void deleteById(String id, boolean hardDelete) {
        ratingEngineEntityMgr.deleteById(id, hardDelete);
        evictRatingMetadataCache();
    }

    @Override
    public void revertDelete(String id) {
        ratingEngineEntityMgr.revertDelete(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false);
        RatingModelService<RatingModel> ratingModelService = RatingModelServiceBase
                .getRatingModelService(ratingEngine.getType());
        return ratingModelService.getAllRatingModelsByRatingEngineId(ratingEngineId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RatingModel getRatingModel(String ratingEngineId, String ratingModelId) {
        RatingEngine ratingEngine = validateRatingEngine(ratingEngineId);
        RatingModelService<RatingModel> ratingModelService = RatingModelServiceBase
                .getRatingModelService(ratingEngine.getType());
        return ratingModelService.geRatingModelById(ratingModelId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RatingModel updateRatingModel(String ratingEngineId, String ratingModelId, RatingModel ratingModel) {
        log.info(String.format("Update ratingModel %s for Rating Engine %s", ratingModelId, ratingEngineId));
        RatingEngine ratingEngine = validateRatingEngine(ratingEngineId);
        RatingModelService<RatingModel> ratingModelService = RatingModelServiceBase
                .getRatingModelService(ratingEngine.getType());
        ratingModel.setId(ratingModelId);
        RatingModel model = ratingModelService.createOrUpdate(ratingModel, ratingEngineId);
        evictRatingMetadataCache();
        return model;
    }

    @Override
    public Map<RatingEngineDependencyType, List<String>> getRatingEngineDependencies(String customerSpace,
            String ratingEngineId) {
        log.info(String.format("Attempting to find rating engine dependencies for Rating Engine %s", ratingEngineId));
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId, false, false);
        if (ratingEngine == null) {
            throw new LedpException(LedpCode.LEDP_40016, new String[] { ratingEngineId, customerSpace });
        }

        HashMap<RatingEngineDependencyType, List<String>> dependencyMap = new HashMap<>();

        // Eventually should be something like this
        // for (RatingEngineDependencyType type :
        // RatingEngineDependencyType.values()) {
        // dependencyMap.put(type, ratingEngine.getDependencies(type));
        // }

        dependencyMap.put(RatingEngineDependencyType.Play, playEntityMgr.findAllByRatingEnginePid(ratingEngine.getPid())
                .stream().map(Play::getDisplayName).collect(Collectors.toList()));

        return dependencyMap;
    }

    @Override
    public EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version) {
        if (ratingModel == null) {
            throw new LedpException(LedpCode.LEDP_40014, new String[] { ratingEngine.getId(), customerSpace });
        }

        if (ratingEngine.getType() == RatingEngineType.CROSS_SELL && ratingModel instanceof AIModel) {
            AIModelService aiModelService = (AIModelService) RatingModelServiceBase
                    .getRatingModelService(ratingEngine.getType());
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
        switch (modelingQueryType) {
        case TARGET:
            return eventProxy.getScoringCount(customerSpace, efeq);
        case TRAINING:
            return eventProxy.getTrainingCount(customerSpace, efeq);
        case EVENT:
            return eventProxy.getEventCount(customerSpace, efeq);
        default:
            throw new LedpException(LedpCode.LEDP_40010, new String[] { modelingQueryType.getModelingQueryTypeName() });
        }
    }

    @Override
    public String modelRatingEngine(String customerSpace, RatingEngine ratingEngine, AIModel aiModel,
            String userEmail) {
        if (ratingEngine.getType() == RatingEngineType.RULE_BASED) {
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { RatingEngineType.RULE_BASED.getRatingEngineTypeName() });
        }
        ApplicationId jobId = aiModel.getModelingYarnJobId();
        if (jobId != null) {
            return jobId.toString();
        }

        switch (ratingEngine.getType()) {
        case RULE_BASED:
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { RatingEngineType.RULE_BASED.getRatingEngineTypeName() });
        case CROSS_SELL:
            if (CollectionUtils
                    .isEmpty(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts())) {
                throw new LedpException(LedpCode.LEDP_40012,
                        new String[] { aiModel.getId(), CustomerSpace.parse(customerSpace).toString() });
            }
            internalResourceProxy.setModelSummaryDownloadFlag(CustomerSpace.parse(customerSpace).toString());
            DataCollection.Version activeVersion = dataCollectionService.getActiveVersion(customerSpace);
            RatingEngineModelingParameters parameters = new RatingEngineModelingParameters();
            parameters.setName(aiModel.getId());
            parameters.setDisplayName(ratingEngine.getDisplayName() + "_" + aiModel.getIteration());
            parameters.setDescription(ratingEngine.getDisplayName());
            parameters.setModuleName("Module");
            parameters.setUserId(userEmail);
            parameters.setRatingEngineId(ratingEngine.getId());
            parameters.setAiModelId(aiModel.getId());
            parameters.setTargetFilterQuery(
                    getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.TARGET, activeVersion));
            parameters.setTargetFilterTableName(aiModel.getId() + "_target");
            parameters.setTrainFilterQuery(
                    getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.TRAINING, activeVersion));
            parameters.setTrainFilterTableName(aiModel.getId() + "_train");
            parameters.setEventFilterQuery(
                    getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.EVENT, activeVersion));
            parameters.setEventFilterTableName(aiModel.getId() + "_event");
            if (aiModel.getPredictionType() == PredictionType.EXPECTED_VALUE) {
                parameters.setExpectedValue(true);
            }

            log.info(String.format("Cross-sell modelling job submitted with parameters %s", parameters.toString()));
            jobId = ratingEngineImportMatchAndModelWorkflowSubmitter.submit(parameters);
            break;
        case CUSTOM_EVENT:
            CustomEventModelingConfig config = (CustomEventModelingConfig) aiModel.getAdvancedModelingConfig();
            ModelingParameters modelingParameters = new ModelingParameters();
            modelingParameters.setName(aiModel.getId());
            modelingParameters.setDisplayName(ratingEngine.getDisplayName() + "_" + aiModel.getIteration());
            modelingParameters.setDescription(ratingEngine.getDisplayName());
            modelingParameters.setModuleName("Module");
            modelingParameters.setUserId(userEmail);
            modelingParameters.setRatingEngineId(ratingEngine.getId());
            modelingParameters.setAiModelId(aiModel.getId());
            modelingParameters.setCustomEventModelingType(config.getCustomEventModelingType());
            modelingParameters.setFilename(config.getSourceFileName());
            modelingParameters.setActivateModelSummaryByDefault(true);
            modelingParameters.setDeduplicationType(config.getDeduplicationType());
            modelingParameters.setExcludePublicDomains(config.isExcludePublicDomains());
            modelingParameters.setTransformationGroup(config.getConvertedTransformationGroup());
            modelingParameters.setExcludePropDataColumns(
                    !config.getDataStores().contains(CustomEventModelingConfig.DataStore.DataCloud));
            modelingParameters
                    .setExcludeCDLAttributes(!config.getDataStores().contains(CustomEventModelingConfig.DataStore.CDL));
            modelingParameters.setExcludeCustomFileAttributes(
                    !config.getDataStores().contains(CustomEventModelingConfig.DataStore.CustomFileAttributes));
            internalResourceProxy.setModelSummaryDownloadFlag(CustomerSpace.parse(customerSpace).toString());

            log.info(String.format("Custom event modelling job submitted with parameters %s",
                    modelingParameters.toString()));
            jobId = customEventModelingWorkflowSubmitter.submit(CustomerSpace.parse(customerSpace).toString(),
                    modelingParameters);
            break;
        default:
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { ratingEngine.getType().getRatingEngineTypeName() });
        }

        aiModel.setModelingJobId(jobId.toString());
        updateRatingModel(ratingEngine.getId(), aiModel.getId(), aiModel);
        return jobId.toString();
    }

    @VisibleForTesting
    Set<String> engineIdsAvailableInRedshift() {
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
            createOrUpdate(ratingEngine, tenant.getId());
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
        String tenantId = MultiTenantContext.getTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        String keyPrefix = tenantId + "|" + BusinessEntity.Rating.name();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.DataCloudCMCache);
        servingStoreCacheService.clearCache(tenantId, BusinessEntity.Rating);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AttributeLookup> getDependentAttrsInAllModels(String ratingEngineId) {
        Set<AttributeLookup> attributes = new HashSet<>();
        RatingEngine ratingEngine = validateRatingEngine(ratingEngineId);
        RatingModelService<RatingModel> ratingModelService = RatingModelServiceBase
                .getRatingModelService(ratingEngine.getType());

        List<RatingModel> ratingModels = ratingModelService.getAllRatingModelsByRatingEngineId(ratingEngineId);
        if (ratingModels != null) {
            for (RatingModel ratingModel : ratingModels) {
                ratingModelService.findRatingModelAttributeLookups(ratingModel);
                attributes.addAll(ratingModel.getRatingModelAttributes());
            }
        }

        return new ArrayList<>(attributes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AttributeLookup> getDependentAttrsInActiveModel(String ratingEngineId) {
        Set<AttributeLookup> attributes = new HashSet<>();
        RatingEngine ratingEngine = validateRatingEngine_PopulateActiveModel(ratingEngineId);
        RatingModelService<RatingModel> ratingModelService = RatingModelServiceBase
                .getRatingModelService(ratingEngine.getType());

        RatingModel activeRatingModel = ratingEngine.getActiveModel();
        if (activeRatingModel != null) {
            ratingModelService.findRatingModelAttributeLookups(activeRatingModel);
            attributes.addAll(activeRatingModel.getRatingModelAttributes());
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
            for (RatingEngine ratingEngine : ratingEngines) {
                ratingModelService = RatingModelServiceBase.getRatingModelService(ratingEngine.getType());

                List<RatingModel> ratingModels = ratingModelService
                        .getAllRatingModelsByRatingEngineId(ratingEngine.getId());
                if (ratingModels != null) {
                    for (RatingModel ratingModel : ratingModels) {
                        ratingModelService.findRatingModelAttributeLookups(ratingModel);
                        for (AttributeLookup modelAttribute : ratingModel.getRatingModelAttributes()) {
                            if (attributes.contains(sanitize(modelAttribute.toString()))) {
                                ratingModelSet.add(ratingModel);
                                break;
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
        RatingModelService<RatingModel> ratingModelService;
        List<RatingEngine> ratingEngines = getAllRatingEngines();
        if (ratingEngines != null) {
            for (RatingEngine ratingEngine : ratingEngines) {
                ratingModelService = RatingModelServiceBase.getRatingModelService(ratingEngine.getType());

                List<RatingModel> ratingModels = ratingModelService
                        .getAllRatingModelsByRatingEngineId(ratingEngine.getId());
                if (ratingModels != null) {
                    rm: for (RatingModel ratingModel : ratingModels) {
                        ratingModelService.findRatingModelAttributeLookups(ratingModel);
                        for (AttributeLookup modelAttribute : ratingModel.getRatingModelAttributes()) {
                            if (attributes.contains(sanitize(modelAttribute.toString()))) {
                                ratingEngineSet.add(ratingEngine);
                                break rm;
                            }
                        }
                    }
                }
            }
        }

        return new ArrayList<>(ratingEngineSet);
    }

    private String sanitize(String attribute) {
        if (StringUtils.isNotBlank(attribute)) {
            attribute = attribute.trim();
        }
        return attribute;
    }
}
