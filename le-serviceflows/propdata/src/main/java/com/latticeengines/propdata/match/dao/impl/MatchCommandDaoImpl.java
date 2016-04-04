package com.latticeengines.propdata.match.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.propdata.match.dao.MatchCommandDao;

@Component("matchCommandDao")
public class MatchCommandDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<MatchCommand>
        implements MatchCommandDao {

    @Override
    protected Class<MatchCommand> getEntityClass() {
        return MatchCommand.class;
    }

}
