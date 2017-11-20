package com.latticeengines.dante.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.multitenant.PublishedTalkingPoint;

public interface PublishedTalkingPointDao extends BaseDao<PublishedTalkingPoint> {
    List<PublishedTalkingPoint> findAllByPlayName(String playName);
}
