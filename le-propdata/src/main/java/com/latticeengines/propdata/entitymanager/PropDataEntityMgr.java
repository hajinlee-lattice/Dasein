package com.latticeengines.propdata.entitymanager;

import com.latticeengines.domain.exposed.propdata.CommandIds;
import com.latticeengines.domain.exposed.propdata.Commands;


public interface PropDataEntityMgr {

	void createCommands(Commands commands);
	Commands getCommands(Long pid);
	CommandIds getCommandIds(Long pid);
	void dropTable(String tableName);
	void createTableByQuery(String sql);
}
