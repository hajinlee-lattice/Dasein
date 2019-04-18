package com.latticeengines.datacloud.madison.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgress;
import com.latticeengines.domain.exposed.datacloud.MadisonLogicDailyProgressStatus;

public class MadisonLogicDailyProgressDaoImpl extends BaseDaoImpl<MadisonLogicDailyProgress> implements
        com.latticeengines.datacloud.madison.dao.MadisonLogicDailyProgressDao {

    @Override
    protected Class<MadisonLogicDailyProgress> getEntityClass() {
        return MadisonLogicDailyProgress.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public MadisonLogicDailyProgress getNextAvailableDailyProgress() {
        Session session = getSessionFactory().getCurrentSession();
        Class<MadisonLogicDailyProgress> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where status = :status order by ID",
                entityClz.getSimpleName());
        Query<MadisonLogicDailyProgress> query = session.createQuery(queryStr);
        query.setParameter("status", MadisonLogicDailyProgressStatus.DEPIVOTED.getStatus());
        query.setMaxResults(1);
        List<MadisonLogicDailyProgress> list = query.list();
        if (!CollectionUtils.isEmpty(list)) {
            return list.get(0);
        }

        queryStr = String.format(
                "from %s where status = :status and FileDate >= DATEADD(DAY, -7, GETDATE()) order by ID",
                entityClz.getSimpleName());
        query = session.createQuery(queryStr);
        query.setParameter("status", MadisonLogicDailyProgressStatus.FAILED.getStatus());
        query.setMaxResults(1);
        list = query.list();
        if (!CollectionUtils.isEmpty(list)) {
            return list.get(0);
        } else {
            return null;
        }

    }

}
