package com.latticeengines.datacloud.match.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

public interface MatchCommandEntityMgr {

    MatchCommand createCommand(MatchCommand command);
    MatchCommand updateCommand(MatchCommand command);
    MatchCommand findByRootOperationUid(String rootUid);
    List<MatchCommand> findOutDatedCommands(int retentionDays);
    void deleteCommand(MatchCommand command);

}
