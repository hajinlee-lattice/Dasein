package com.latticeengines.propdata.api.entitymanager;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.CommandId;

public interface CommandEntityMgr {

    void createCommand(Command command);

    Command getCommand(Long pid);

    CommandId getCommandId(Long pid);
}
