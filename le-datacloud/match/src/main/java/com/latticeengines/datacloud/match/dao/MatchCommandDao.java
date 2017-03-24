package com.latticeengines.datacloud.match.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

public interface MatchCommandDao extends BaseDao<MatchCommand> {
    List<MatchCommand> findOutDatedCommands(int retentionDays);
    void deleteCommand(MatchCommand command);
}
