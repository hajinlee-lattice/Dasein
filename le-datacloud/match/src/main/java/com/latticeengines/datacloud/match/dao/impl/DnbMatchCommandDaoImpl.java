package com.latticeengines.datacloud.match.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.DnbMatchCommandDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

@Component("dnbMatchCommandDao")
public class DnbMatchCommandDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<DnBMatchCommand> implements DnbMatchCommandDao {

    @Override
    protected Class<DnBMatchCommand> getEntityClass() {
        return DnBMatchCommand.class;
    }

}
