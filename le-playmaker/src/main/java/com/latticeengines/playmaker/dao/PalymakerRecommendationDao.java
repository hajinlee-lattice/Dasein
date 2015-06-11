package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.dao.GenericDao;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

public interface PalymakerRecommendationDao extends GenericDao {

    List<Map<String, Object>> getRecommendations(int startRecord, int size);

    List<Map<String, Object>> getPlays(int startId, int size);
}
