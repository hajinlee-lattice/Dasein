package com.latticeengines.datacloud.match.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

public interface MatchCommandEntityMgr {

    MatchCommand createCommand(MatchCommand command);
    MatchCommand updateCommand(MatchCommand command);
    MatchCommand findByRootOperationUid(String rootUid);

}
