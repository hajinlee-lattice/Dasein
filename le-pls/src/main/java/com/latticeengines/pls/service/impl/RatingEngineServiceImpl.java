package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.pls.service.RatingModelService;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("ratingEngineService")
public class RatingEngineServiceImpl implements RatingEngineService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineServiceImpl.class);

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    @Autowired
    private DataFeedProxy dataFeedProxy;

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
        ratingEngineEntityMgr.findAllByTypeAndStatus(type, status).stream()
                .forEach(re -> result.add(constructRatingEngineSummary(re, tenant.getId())));
        return result;
    }

    @VisibleForTesting
    RatingEngineSummary constructRatingEngineSummary(RatingEngine ratingEngine, String tenantId) {
        if (ratingEngine == null) {
            return null;
        }
        RatingEngineSummary ratingEngineSummary = new RatingEngineSummary();
        ratingEngineSummary.setId(ratingEngine.getId());
        ratingEngineSummary.setDisplayName(ratingEngine.getDisplayName());
        ratingEngineSummary.setNote(ratingEngine.getNote());
        ratingEngineSummary.setType(ratingEngine.getType());
        ratingEngineSummary.setStatus(ratingEngine.getStatus());
        ratingEngineSummary.setSegmentDisplayName(
                ratingEngine.getSegment() != null ? ratingEngine.getSegment().getDisplayName() : null);
        ratingEngineSummary.setCreatedBy(ratingEngine.getCreatedBy());
        ratingEngineSummary.setCreated(ratingEngine.getCreated());
        ratingEngineSummary.setUpdated(ratingEngine.getUpdated());

        Date lastRefreshedDate = findLastRefreshedDate(tenantId);
        ratingEngineSummary.setLastRefreshedDate(lastRefreshedDate);
        return ratingEngineSummary;
    }

    @Override
    public RatingEngine getRatingEngineById(String id) {
        Tenant tenant = MultiTenantContext.getTenant();
        RatingEngine ratingEngine = ratingEngineEntityMgr.findById(id);
        updateLastRefreshedDate(tenant.getId(), ratingEngine);
        return ratingEngine;
    }

    @Override
    public RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId) {
        if (ratingEngine == null) {
            throw new NullPointerException("Entity is null when creating a rating engine.");
        }
        Tenant tenant = MultiTenantContext.getTenant();
        if (ratingEngine.getSegment() != null) {
            MetadataSegmentDTO segmentDTO = metadataSegmentService
                    .getSegmentDTOByName(ratingEngine.getSegment().getName(), false);
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

    @Override
    public Set<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId);
        return ratingEngine.getRatingModels();
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
        return ratingModelService.createOrUpdate(ratingModel, ratingEngineId);
    }

    private void updateLastRefreshedDate(String tenantId, List<RatingEngine> ratingEngines) {
        if (CollectionUtils.isNotEmpty(ratingEngines)) {
            Date lastRefreshedDate = findLastRefreshedDate(tenantId);
            ratingEngines.stream().forEach(re -> re.setLastRefreshedDate(lastRefreshedDate));
        }
    }

    private void updateLastRefreshedDate(String tenantId, RatingEngine ratingEngine) {
        if (ratingEngine != null) {
            Date lastRefreshedDate = findLastRefreshedDate(tenantId);
            ratingEngine.setLastRefreshedDate(lastRefreshedDate);
        }
    }

    private Date findLastRefreshedDate(String tenantId) {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(tenantId);
        Date lastRefreshedDate = dataFeed.getLastPublished();
        return lastRefreshedDate;
    }

    private RatingEngine validateRatingEngine(String ratingEngineId) {

        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId);
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
