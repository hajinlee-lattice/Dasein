package com.latticeengines.datadb.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datadb.Recommendation;

public interface RecommendationDao extends BaseDao<Recommendation> {

    Recommendation findByLaunchId(String launchId);

}
