package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;

public interface AIModelEntityMgr extends BaseEntityMgrRepository<AIModel, Long> {

    AIModel createOrUpdateAIModel(AIModel aiModel, String ratingEngineId);

    List<AIModel> findAllByRatingEngineId(String ratingEngineId);

    List<AIModel> findAllByRatingEngineId(String ratingEngineId, Pageable pageable);

    AIModel findById(String id);

    void deleteById(String id);

    MetadataSegment inflateParentSegment(AIModel aiModel);

}
