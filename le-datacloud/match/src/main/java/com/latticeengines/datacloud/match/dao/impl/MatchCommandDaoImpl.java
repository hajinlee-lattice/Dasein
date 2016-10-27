package com.latticeengines.datacloud.match.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.MatchCommandDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

@Component("matchCommandDao")
public class MatchCommandDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<MatchCommand>
        implements MatchCommandDao {

    @Override
    protected Class<MatchCommand> getEntityClass() {
        return MatchCommand.class;
    }

}
