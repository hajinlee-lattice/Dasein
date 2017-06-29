package com.latticeengines.datadb.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.datadb.Recommendation;

public interface RecommendationEntityMgr extends BaseEntityMgr<Recommendation> {

    void create(Recommendation entity);

    Recommendation findByLaunchId(String launchId);

    void delete(Recommendation entity);
}
