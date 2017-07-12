package com.latticeengines.dante.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.TalkingPoint;

public interface TalkingPointDao extends BaseDao<TalkingPoint> {

    List<TalkingPoint> findAllByPlayID(Long playID);
}
