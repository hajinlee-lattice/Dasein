package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface RatingEngineDao extends BaseDao<RatingEngine> {

    RatingEngine findById(String id);

    List<RatingEngine> findAllByTypeAndStatus(String type, String status);

    List<String> findAllIdsInSegment(String segmentName);

    void deleteById(String id);

    List<String> findAllActiveModels();

}
