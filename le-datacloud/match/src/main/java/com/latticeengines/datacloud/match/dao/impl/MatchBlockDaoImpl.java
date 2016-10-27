package com.latticeengines.datacloud.match.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.MatchBlockDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;

@Component("matchOperationDao")
public class MatchBlockDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<MatchBlock>
        implements MatchBlockDao {

    @Override
    protected Class<MatchBlock> getEntityClass() {
        return MatchBlock.class;
    }

}
