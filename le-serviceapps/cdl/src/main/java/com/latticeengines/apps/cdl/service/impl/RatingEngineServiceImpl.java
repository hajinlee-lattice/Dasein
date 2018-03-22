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
import com.latticeengines.apps.cdl.service.PeriodService;
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
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.RatingEngineFrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

import reactor.core.publisher.ParallelFlux;

@Component("ratingEngineService")
public class RatingEngineServiceImpl extends RatingEngineTemplate implements RatingEngineService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineServiceImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

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
    private PeriodService periodService;

    @Inject
    private RatingEngineImportMatchAndModelWorkflowSubmitter ratingEngineImportMatchAndModelWorkflowSubmitter;

    @Inject
    private CustomEventModelingWorkflowSubmitter customEventModelingWorkflowSubmitter;

    @Inject
    private TableRoleTemplate tableRoleTemplate;

    @Inject
    private DataCollectionService dataCollectionService;

    @Override
    public List<RatingEngine> getAllRatingEngines() {
        Tenant tenant = MultiTenantContext.getTenant();
        List<RatingEngine> result = ratingEngineEntityMgr.findAll();
        updateLastRefreshedDate(tenant.getId(), result);
        return result;
    }

    @Override
    public List<RatingEngineSummary> getAllRatingEngineSummaries() {
        return getAllRatingEngineSummariesWithTypeAndStatus(null, null);
    }

    @Override
    public List<RatingEngineSummary> getAllRatingEngineSummariesWithTypeAndStatus(String type, String status) {
        return getAllRatingEngineSummariesWithTypeAndStatusInRedShift(type, status, false);
    }

    @Override
    public List<RatingEngineSummary> getAllRatingEngineSummariesWithTypeAndStatusInRedShift(String type, String status,
            Boolean onlyInRedshift) {
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format(
                "Get all the rating engine summaries for tenant %s with status set to %s and type set to %s",
                tenant.getId(), status, type));
        List<RatingEngineSummary> result = new ArrayList<>();
        ratingEngineEntityMgr.findAllByTypeAndStatus(type, status)
                .forEach(re -> result.add(constructRatingEngineSummary(re, tenant.getId())));
        if (onlyInRedshift != null && onlyInRedshift) {
            Set<String> availableRatingIdInRedshift = engineIdsAvailableInRedshift();
            return result.stream()
                    .filter(ratingEngineSummary -> availableRatingIdInRedshift.contains(ratingEngineSummary.getId()))
                    .collect(Collectors.toList());
        } else {
            return result;
        }
    }

    @Override
    public List<String> getAllRatingEngineIdsInSegment(String segmentName) {
        return ratingEngineEntityMgr.findAllIdsInSegment(segmentName);
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

        ratingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine, tenantId);
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
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(id);
        ratingEngineEntityMgr.deleteRatingEngine(ratingEngine, hardDelete);
        evictRatingMetadataCache();
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
            ModelingQueryType modelingQueryType) {
        if (ratingModel == null) {
            throw new LedpException(LedpCode.LEDP_40014, new String[] { ratingEngine.getId(), customerSpace });
        }

        if (ratingEngine.getType() == RatingEngineType.CROSS_SELL && ratingModel instanceof AIModel) {
            AIModelService aiModelService = (AIModelService) RatingModelServiceBase
                    .getRatingModelService(ratingEngine.getType());
            return aiModelService.getModelingQuery(customerSpace, ratingEngine, (AIModel) ratingModel,
                    modelingQueryType);
        } else {
            throw new LedpException(LedpCode.LEDP_40009,
                    new String[] { ratingEngine.getId(), ratingModel.getId(), customerSpace });
        }
    }

    @Override
    public Long getModelingQueryCount(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType) {

        EventFrontEndQuery efeq = getModelingQuery(customerSpace, ratingEngine, ratingModel, modelingQueryType);
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
        ApplicationId jobId;

        switch (ratingEngine.getType()) {
        case RULE_BASED:
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { RatingEngineType.RULE_BASED.getRatingEngineTypeName() });
        case CROSS_SELL:
            jobId = aiModel.getModelingYarnJobId();
            if (jobId == null) {
                if (CollectionUtils
                        .isEmpty(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts())) {
                    throw new LedpException(LedpCode.LEDP_40012,
                            new String[] { aiModel.getId(), CustomerSpace.parse(customerSpace).toString() });
                }
                internalResourceProxy.setModelSummaryDownloadFlag(CustomerSpace.parse(customerSpace).toString());
                RatingEngineModelingParameters parameters = new RatingEngineModelingParameters();
                parameters.setName(aiModel.getId());
                parameters.setDisplayName(ratingEngine.getDisplayName() + "_" + aiModel.getIteration());
                parameters.setDescription(ratingEngine.getDisplayName());
                parameters.setModuleName("Module");
                parameters.setUserId(userEmail);
                parameters.setRatingEngineId(ratingEngine.getId());
                parameters.setAiModelId(aiModel.getId());
                parameters.setTargetFilterQuery(
                        getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.TARGET));
                parameters.setTargetFilterTableName(aiModel.getId() + "_target");
                parameters.setTrainFilterQuery(
                        getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.TRAINING));
                parameters.setTrainFilterTableName(aiModel.getId() + "_train");
                parameters.setEventFilterQuery(
                        getModelingQuery(customerSpace, ratingEngine, aiModel, ModelingQueryType.EVENT));
                parameters.setEventFilterTableName(aiModel.getId() + "_event");

                if (aiModel.getPredictionType() == PredictionType.EXPECTED_VALUE) {
                    parameters.setExpectedValue(true);
                }

                log.info(String.format("Cross-sell modelling job submitted with parameters %s", parameters.toString()));
                jobId = ratingEngineImportMatchAndModelWorkflowSubmitter.submit(parameters);

                aiModel.setModelingJobId(jobId.toString());
                updateRatingModel(ratingEngine.getId(), aiModel.getId(), aiModel);
            }
            return jobId.toString();
        case CUSTOM_EVENT:
            jobId = aiModel.getModelingYarnJobId();
            if (jobId == null) {
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
                modelingParameters.setExcludePropDataColumns(
                        !config.getDataStores().contains(CustomEventModelingConfig.DataStore.DataCloud));
                modelingParameters.setExcludeCDLAttributes(
                        !config.getDataStores().contains(CustomEventModelingConfig.DataStore.CDL));
                modelingParameters.setExcludeCustomFileAttributes(
                        !config.getDataStores().contains(CustomEventModelingConfig.DataStore.CustomFileAttributes));
                internalResourceProxy.setModelSummaryDownloadFlag(CustomerSpace.parse(customerSpace).toString());

                log.info(String.format("Custom event modelling job submitted with parameters %s",
                        modelingParameters.toString()));
                jobId = customEventModelingWorkflowSubmitter.submit(customerSpace, modelingParameters);
            }
            return jobId.toString();
        default:
            throw new LedpException(LedpCode.LEDP_31107,
                    new String[] { ratingEngine.getType().getRatingEngineTypeName() });
        }
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
        if (ratingEngine == null) {
            throw new NullPointerException(String.format("Rating Engine with id %s is null", ratingEngineId));
        }
        RatingEngineType type = ratingEngine.getType();
        if (type == null) {
            throw new LedpException(LedpCode.LEDP_18154, new String[] { ratingEngineId });
        }
        return ratingEngine;
    }

    private void evictRatingMetadataCache() {
        String tenantId = MultiTenantContext.getTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        String keyPrefix = tenantId + "|" + BusinessEntity.Rating.name();
        cacheService.refreshKeysByPattern(keyPrefix, CacheName.DataCloudCMCache);
    }

}
