package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.RatingEngineService;

@Component("ratingEngineService")
public class RatingEngineServiceImpl implements RatingEngineService {

    private static Logger log = LoggerFactory.getLogger(RatingEngineServiceImpl.class);

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Autowired
    private MetadataSegmentService metadataSegmentService;

    @Override
    public List<RatingEngine> getAllRatingEngines() {
        return ratingEngineEntityMgr.findAll();
    }

    @Override
    public RatingEngine getRatingEngineById(String id) {
        return ratingEngineEntityMgr.findById(id);
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
    public Set<RatingModel> getAllRatingModelsById(String id) {
        RatingEngine ratingEngine = getRatingEngineById(id);
        return ratingEngine.getRatingModels();
    }

}
