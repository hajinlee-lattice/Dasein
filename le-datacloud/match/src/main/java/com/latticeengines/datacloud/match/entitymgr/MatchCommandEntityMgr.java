package com.latticeengines.datacloud.match.entitymgr;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

import java.util.List;

public interface MatchCommandEntityMgr {

    MatchCommand createCommand(MatchCommand command);
    MatchCommand updateCommand(MatchCommand command);
    MatchCommand findByRootOperationUid(String rootUid);
    List<MatchCommand> findOutDatedCommands(int retentionDays);
    void deleteCommand(MatchCommand command);

}
