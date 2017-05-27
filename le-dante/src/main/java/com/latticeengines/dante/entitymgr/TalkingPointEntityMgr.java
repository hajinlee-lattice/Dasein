package com.latticeengines.dante.entitymgr;

import java.util.List;

import com.latticeengines.dantedb.exposed.entitymgr.BaseDanteEntityMgr;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

public interface TalkingPointEntityMgr extends BaseDanteEntityMgr<DanteTalkingPoint> {
    DanteTalkingPoint findByExternalID(String externalId);

    List<DanteTalkingPoint> findAllByPlayID(String playExternalID);
}
