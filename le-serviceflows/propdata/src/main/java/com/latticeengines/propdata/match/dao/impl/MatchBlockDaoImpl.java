package com.latticeengines.propdata.match.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.MatchBlock;
import com.latticeengines.propdata.match.dao.MatchBlockDao;

@Component("matchOperationDao")
public class MatchBlockDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<MatchBlock>
        implements MatchBlockDao {

    @Override
    protected Class<MatchBlock> getEntityClass() {
        return MatchBlock.class;
    }

}
