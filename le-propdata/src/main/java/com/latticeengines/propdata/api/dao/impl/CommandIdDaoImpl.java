package com.latticeengines.propdata.api.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.CommandId;
import com.latticeengines.propdata.api.dao.CommandIdDao;

public class CommandIdDaoImpl extends BaseDaoImpl<CommandId> implements CommandIdDao {

    public CommandIdDaoImpl() {
        super();
    }

    @Override
    protected Class<CommandId> getEntityClass() {
        return CommandId.class;
    }

}