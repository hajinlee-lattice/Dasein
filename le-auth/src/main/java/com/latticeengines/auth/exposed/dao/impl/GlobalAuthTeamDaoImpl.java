package com.latticeengines.auth.exposed.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthTeamDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

@Component("globalAuthTeamDao")
public class GlobalAuthTeamDaoImpl extends BaseDaoImpl<GlobalAuthTeam> implements GlobalAuthTeamDao {

    @Override
    protected Class<GlobalAuthTeam> getEntityClass() {
        return GlobalAuthTeam.class;
    }
}
