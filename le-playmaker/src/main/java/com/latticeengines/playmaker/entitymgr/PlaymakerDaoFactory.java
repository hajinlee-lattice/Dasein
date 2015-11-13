package com.latticeengines.playmaker.entitymgr;

import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;

public interface PlaymakerDaoFactory {

    PlaymakerRecommendationDao getRecommendationDao(String tenantName);

}
