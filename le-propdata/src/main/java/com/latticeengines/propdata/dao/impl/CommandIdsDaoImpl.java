package com.latticeengines.propdata.dao.impl;

import com.latticeengines.dataplatform.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.CommandIds;
import com.latticeengines.propdata.dao.CommandIdsDao;

public class CommandIdsDaoImpl extends BaseDaoImpl<CommandIds> implements CommandIdsDao {

    public CommandIdsDaoImpl() {
        super();
    }

    @Override
    protected Class<CommandIds> getEntityClass() {
        return CommandIds.class;
    }


}