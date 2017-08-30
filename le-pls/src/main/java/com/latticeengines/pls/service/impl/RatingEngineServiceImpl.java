package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.RatingEngineService;
import com.latticeengines.pls.service.RatingModelService;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

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
        return ratingEngineEntityMgr.findAll();
    }

    @Override
    public List<RatingEngineSummary> getAllRatingEngineSummaries() {
        List<RatingEngineSummary> result = new ArrayList<>();
        ratingEngineEntityMgr.findAll().stream().forEach(re -> result.add(constructRatingEngineSummary(re.getId())));
        return result;
    }

    @VisibleForTesting
    RatingEngineSummary constructRatingEngineSummary(String ratingEngineId) {
        if (ratingEngineId == null) {
            return null;
        }
        RatingEngine ratingEngine = getFullRatingEngineById(ratingEngineId);
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

        DataFeed dataFeed = dataFeedProxy.getDataFeed(ratingEngine.getTenant().getId());
        ratingEngineSummary.setLastRefreshedDate(dataFeed.getLastPublished());
        return ratingEngineSummary;
    }

    @Override
    public RatingEngine getRatingEngineById(String id) {
        return ratingEngineEntityMgr.findById(id);
    }

    @Override
    public RatingEngine getFullRatingEngineById(String id) {
        return ratingEngineEntityMgr.findById(id, true, true, true);
    }

    @Override
    public RatingEngine createOrUpdate(RatingEngine ratingEngine, String tenantId) {
        if (ratingEngine == null) {
            throw new NullPointerException("Entity is null when creating a rating engine.");
        }
        if (ratingEngine.getSegment() != null) {
            MetadataSegment segment = metadataSegmentService.getSegmentByName(ratingEngine.getSegment().getName(),
                    false);
            ratingEngine.setSegment(segment);
        }
        return ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine, tenantId);
    }

    @Override
    public void deleteById(String id) {
        ratingEngineEntityMgr.deleteById(id);
    }

    @Override
    public Set<RatingModel> getRatingModelsByRatingEngineId(String ratingEngineId) {
        RatingEngine ratingEngine = getFullRatingEngineById(ratingEngineId);
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
