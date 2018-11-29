package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;

public interface TalkingPointDao extends BaseDao<TalkingPoint> {
    List<TalkingPoint> findAllByPlayName(String playName);
}
