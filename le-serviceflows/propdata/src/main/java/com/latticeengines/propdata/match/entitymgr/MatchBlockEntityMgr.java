package com.latticeengines.propdata.match.entitymgr;

import com.latticeengines.domain.exposed.propdata.manage.MatchBlock;

public interface MatchBlockEntityMgr {

    MatchBlock createBlock(MatchBlock command);
    MatchBlock updateBlock(MatchBlock command);
    MatchBlock findByBlockUid(String blockUid);

}
