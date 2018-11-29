package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.cdl.PublishedTalkingPoint;

public interface PublishedTalkingPointDao extends BaseDao<PublishedTalkingPoint> {
    List<PublishedTalkingPoint> findAllByPlayName(String playName);
}
