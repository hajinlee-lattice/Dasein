package com.latticeengines.dantedb.exposed.entitymgr;

import com.latticeengines.domain.exposed.dantetalkingpoints.DanteTalkingPoint;

public interface TalkingPointEntityMgr {

    void create(DanteTalkingPoint dtp);

    void delete(DanteTalkingPoint dtp);

    DanteTalkingPoint findByExternalID(String externalId);

}
