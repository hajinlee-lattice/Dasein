package com.latticeengines.datacloud.match.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.Commands;
import com.latticeengines.domain.exposed.datacloud.MatchCommandStatus;

public interface CommandDao extends BaseDao<Commands> {

    Commands createCommandByStoredProcedure(String sourceTable, String contractExternalID, String destTables);

    MatchCommandStatus getMatchCommandStatus(Long commandID);

}
