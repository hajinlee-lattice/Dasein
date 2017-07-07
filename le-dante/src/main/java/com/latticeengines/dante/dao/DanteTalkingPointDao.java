package com.latticeengines.dante.dao;

import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

import java.util.List;

public interface DanteTalkingPointDao extends BaseDanteDao<DanteTalkingPoint> {
    List<DanteTalkingPoint> findAllByPlayID(String playID);
}
