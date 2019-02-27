package com.latticeengines.datacloud.match.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.DnbMatchCommandDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

@Component("dnbMatchCommandDao")
public class DnbMatchCommandDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<DnBMatchCommand> implements DnbMatchCommandDao {

    @Override
    protected Class<DnBMatchCommand> getEntityClass() {
        return DnBMatchCommand.class;
    }

    @Override
    public void abandonCommands(String rootOperationUid) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "update %s set dnbCode = :abandoned, message = :abandonMessage where rootOperationUid = :rootOperationUid and dnbCode = :submitted",
                getEntityClass().getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("abandoned", DnBReturnCode.ABANDONED);
        query.setParameter("rootOperationUid", rootOperationUid);
        query.setParameter("submitted", DnBReturnCode.SUBMITTED);
        query.setParameter("abandonMessage", DnBReturnCode.ABANDONED.getMessage());
        query.executeUpdate();
    }
}
