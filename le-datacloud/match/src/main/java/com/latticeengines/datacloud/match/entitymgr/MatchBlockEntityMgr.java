package com.latticeengines.datacloud.match.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;

public interface MatchBlockEntityMgr {

    MatchBlock createBlock(MatchBlock command);
    MatchBlock updateBlock(MatchBlock command);
    MatchBlock findByBlockUid(String blockUid);

}
