package com.latticeengines.propdata.api.entitymanager;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.CommandId;

public interface PropDataEntityMgr {

    void createCommands(Command commands);

    Command getCommands(Long pid);

    CommandId getCommandIds(Long pid);

    void dropTable(String tableName);

    void executeQueryUpdate(String sql);

    void executeProcedure(String procedure);
}
