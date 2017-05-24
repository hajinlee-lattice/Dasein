package com.latticeengines.dantedb.exposed.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dantedb.exposed.dao.DanteTalkingPointsDao;
import com.latticeengines.domain.exposed.dantetalkingpoints.DanteTalkingPoint;

@Component("danteTalkingPointsDao")
public class DanteTalkingPointsDaoImpl extends BaseDanteDaoImpl<DanteTalkingPoint> implements DanteTalkingPointsDao {

    @Override
    protected Class<DanteTalkingPoint> getEntityClass() {
        return DanteTalkingPoint.class;
    }
}
