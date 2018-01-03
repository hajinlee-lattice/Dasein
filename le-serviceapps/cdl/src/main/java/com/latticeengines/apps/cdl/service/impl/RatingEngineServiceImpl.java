package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingModelService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("ratingEngineService")
public class RatingEngineServiceImpl extends RatingEngineTemplate implements RatingEngineService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineServiceImpl.class);

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private EntityProxy entityProxy;

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
        RatingEngine ratingEngine = getRatingEngineById(engineId, false, true);
        if (ratingEngine != null) {
            //TODO: only update rule based for now
            if (RatingEngineType.RULE_BASED.equals(ratingEngine.getType())) {
                RatingModel ratingModel = ratingEngine.getActiveModel();
                Map<String, Long> counts = updateRatingCount(ratingEngine, ratingModel);
                log.info("Updated counts for rating engine " + engineId + " using model " + ratingModel.getId() + " to "
                        + JsonUtils.serialize(counts));
                return counts;
            }
        }
        return null;
    }

    @Override
    public RatingEngine getRatingEngineById(String ratingEngineId, boolean populateRefreshedDate,
            boolean populateActiveModel) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngine ratingEngine = null;
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
        return ratingEngine;
    }

    @Override
    public void deleteById(String id) {
        ratingEngineEntityMgr.deleteById(id);
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
        return updatedModel;
    }

    private Map<String, Long> updateRatingCount(RatingEngine ratingEngine, RatingModel ratingModel) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Cannot find a Tenant in MultiTenantContext, skip getting rating count.");
            return Collections.emptyMap();
        } else {
            MetadataSegment segment = ratingEngine.getSegment();
            FrontEndQuery frontEndQuery = segment != null ? segment.toFrontEndQuery(BusinessEntity.Account)
                    : new FrontEndQuery();
            frontEndQuery.setRatingModels(Collections.singletonList(ratingModel));
            frontEndQuery.setMainEntity(BusinessEntity.Account);
            Map<String, Long> counts = entityProxy.getRatingCount(tenant.getId(), frontEndQuery);
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
}
