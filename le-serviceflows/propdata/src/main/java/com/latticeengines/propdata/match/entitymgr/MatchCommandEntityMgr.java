package com.latticeengines.propdata.match.entitymgr;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;

public interface MatchCommandEntityMgr {

    MatchCommand createCommand(MatchCommand command);
    MatchCommand updateCommand(MatchCommand command);
    MatchCommand findByRootOperationUid(String rootUid);

}
