package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.AIModel;


public interface AIModelEntityMgr extends BaseEntityMgrRepository<AIModel, Long>{

    List<AIModel> findByRatingEngineId(String ratingEngineId, Pageable pageable);

    AIModel findById(String id);
    
    void deleteById(String id);

}
