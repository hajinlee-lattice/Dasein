package com.latticeengines.propdata.eai.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.CommandIds;
import com.latticeengines.propdata.eai.dao.CommandIdsDao;

public class CommandIdsDaoImpl extends BaseDaoImpl<CommandIds> implements CommandIdsDao {

    public CommandIdsDaoImpl() {
        super();
    }

    @Override
    protected Class<CommandIds> getEntityClass() {
        return CommandIds.class;
    }

}