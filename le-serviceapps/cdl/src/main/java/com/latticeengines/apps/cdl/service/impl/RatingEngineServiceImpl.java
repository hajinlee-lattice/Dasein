package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingModelService;
import com.latticeengines.apps.cdl.workflow.RatingEngineImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.cdl.RatingEngineModelingParameters;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

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
    private SegmentProxy segmentProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private EventProxy eventProxy;

    @Inject
    private RatingEngineImportMatchAndModelWorkflowSubmitter ratingEngineImportMatchAndModelWorkflowSubmitter;

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
        Tenant tenant = MultiTenantContext.getTenant();
        log.info(String.format(
                "Get all the rating engine summaries for tenant %s with status set to %s and type set to %s",
                tenant.getId(), status, type));
        List<RatingEngineSummary> result = new ArrayList<>();
        ratingEngineEntityMgr.findAllByTypeAndStatus(type, status)
                .forEach(re -> result.add(constructRatingEngineSummary(re, tenant.getId())));
        return result;
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
            // TODO: only update rule based for now
            if (RatingEngineType.RULE_BASED.equals(ratingEngine.getType())) {
                RatingModel ratingModel = ratingEngine.getActiveModel();
                counts = updateRatingCount(ratingEngine, ratingModel);
                log.info("Updated counts for rating engine " + engineId + " using model " + ratingModel.getId() + " to "
                        + JsonUtils.serialize(counts));
                evictRatingEngineCaches();
            }
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
        evictRatingEngineCaches();
        return ratingEngine;
    }

    @Override
    public void deleteById(String id) {
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(id);
        ratingEngineEntityMgr.deleteRatingEngine(ratingEngine);
        evictRatingEngineCaches();
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
        RatingModel updatedModel = ratingModelService.createOrUpdate(ratingModel, ratingEngineId);
        try {
            updateRatingCount(ratingEngine, updatedModel);
        } catch (Exception e) {
            log.warn(String.format("Failed to update rating counts for rating engine %s - rating model %s: %s",
                    ratingEngineId, ratingModelId, e.getMessage()));
        }
        evictRatingEngineCaches();
        return updatedModel;
    }

    @Override
    public EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel,
            ModelingQueryType modelingQueryType) {
        if (ratingModel == null) {
            throw new LedpException(LedpCode.LEDP_40014, new String[] { ratingEngine.getId(), customerSpace });
        }

        if (ratingEngine.getType() == RatingEngineType.AI_BASED && ratingModel instanceof AIModel) {
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
    public String modelRatingEngine(String customerSpace, RatingEngine ratingEngine, RatingModel ratingModel) {
        if (ratingModel instanceof AIModel) {
            AIModel aiModel = (AIModel) ratingModel;
            ApplicationId jobId = aiModel.getModelingJobId();
            if (jobId == null) {
                if (CollectionUtils.isEmpty(((AIModel) ratingModel).getTargetProducts())) {
                    throw new LedpException(LedpCode.LEDP_40012,
                            new String[] { ratingModel.getId(), CustomerSpace.parse(customerSpace).toString() });
                }
                internalResourceProxy.setModelSummaryDownloadFlag(CustomerSpace.parse(customerSpace).toString());
                RatingEngineModelingParameters parameters = new RatingEngineModelingParameters();
                parameters.setName(ratingModel.getId());
                parameters.setDisplayName(ratingEngine.getDisplayName() + "_" + ratingModel.getIteration());
                parameters.setDescription(ratingEngine.getDisplayName());
                parameters.setModuleName("Module");
                parameters.setUserId(MultiTenantContext.getEmailAddress());
                parameters.setRatingEngineId(ratingEngine.getId());
                parameters.setAiModelId(aiModel.getId());
                parameters.setTargetFilterQuery(
                        getModelingQuery(customerSpace, ratingEngine, ratingModel, ModelingQueryType.TARGET));
                parameters.setTargetFilterTableName(ratingModel.getId() + "_target");
                parameters.setTrainFilterQuery(
                        getModelingQuery(customerSpace, ratingEngine, ratingModel, ModelingQueryType.TRAINING));
                parameters.setTrainFilterTableName(ratingModel.getId() + "_train");
                parameters.setEventFilterQuery(
                        getModelingQuery(customerSpace, ratingEngine, ratingModel, ModelingQueryType.EVENT));
                parameters.setEventFilterTableName(ratingModel.getId() + "_event");

                if (((AIModel) ratingModel).getPredictionType() == PredictionType.EXPECTED_VALUE) {
                    parameters.setExpectedValue(true);
                }

                log.info(String.format("Rating Engine model called with parameters %s", parameters.toString()));
                jobId = ratingEngineImportMatchAndModelWorkflowSubmitter.submit(parameters);

                ((AIModel) ratingModel).setModelingJobId(jobId.toString());
                updateRatingModel(ratingEngine.getId(), ratingModel.getId(), ratingModel);
            }
            return jobId.toString();
        } else {
            throw new LedpException(LedpCode.LEDP_31107, new String[] { ratingModel.getClass().getName() });
        }
    }

    private Map<String, Long> updateRatingCount(RatingEngine ratingEngine, RatingModel ratingModel) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Cannot find a Tenant in MultiTenantContext, skip getting rating count.");
            return Collections.emptyMap();
        } else {
            if (RatingEngineType.RULE_BASED.equals(ratingEngine.getType())) {
                MetadataSegment segment = ratingEngine.getSegment();
                FrontEndQuery frontEndQuery = segment != null ? segment.toFrontEndQuery(BusinessEntity.Account)
                        : new FrontEndQuery();
                frontEndQuery.setRatingModels(Collections.singletonList(ratingModel));
                frontEndQuery.setMainEntity(BusinessEntity.Account);
                Map<String, Long> counts = entityProxy.getRatingCount(tenant.getId(), frontEndQuery);
                log.info("Updating rating engine " + ratingEngine.getId() + " counts " + JsonUtils.serialize(counts));
                ratingEngine.setCountsByMap(counts);
                createOrUpdate(ratingEngine, tenant.getId());
                return counts;
            } else {
                log.warn("Does not support AI rating engine counts yet.");
                return Collections.emptyMap();
            }
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

    private void evictRatingEngineCaches() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(customerSpace.getTenantId(), CacheName.getRatingEnginesCacheGroup());
    }
}
