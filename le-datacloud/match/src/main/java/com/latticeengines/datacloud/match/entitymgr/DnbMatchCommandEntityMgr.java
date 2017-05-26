package com.latticeengines.datacloud.match.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

public interface DnbMatchCommandEntityMgr {
    void createCommand(DnBMatchCommand record);

    void updateCommand(DnBMatchCommand record);

    void deleteCommand(DnBMatchCommand record);

    DnBMatchCommand findRecordByField(String field, Object value);

    List<DnBMatchCommand> findAllByField(String field, Object value);

    void abandonCommands(String rootOperationUid);
}
