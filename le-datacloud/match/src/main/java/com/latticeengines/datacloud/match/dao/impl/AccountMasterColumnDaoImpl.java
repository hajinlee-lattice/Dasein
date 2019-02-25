package com.latticeengines.datacloud.match.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.AccountMasterColumnDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

@Component("accountMasterColumnDao")
public class AccountMasterColumnDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<AccountMasterColumn>
        implements AccountMasterColumnDao {

    @Override
    protected Class<AccountMasterColumn> getEntityClass() {
        return AccountMasterColumn.class;
    }

    @Override
    public void deleteByIdByDataCloudVersion(String amColumnId, String dataCloudVersion) {
        Session session = getSessionFactory().getCurrentSession();
        Class<AccountMasterColumn> entityClz = getEntityClass();
        String queryStr = String.format(
                "delete from %s where amColumnId = :amColumnId and dataCloudVersion = :dataCloudVersion",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("amColumnId", amColumnId);
        query.setParameter("dataCloudVersion", dataCloudVersion);
        query.executeUpdate();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AccountMasterColumn> findByTag(String tag, String dataCloudVersion) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "from %s where groups like :tag and dataCloudVersion = :dataCloudVersion order by category asc, subcategory asc, PID asc",
                getEntityClass().getSimpleName());
        Query<AccountMasterColumn> query = session.createQuery(queryStr);
        query.setParameter("tag", "%" + tag + "%");
        query.setParameter("dataCloudVersion", dataCloudVersion);
        return query.list();

    }

    @Override
    public AccountMasterColumn findById(String amColumnId, String dataCloudVersion) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                "from %s where amColumnId = :amColumnId and dataCloudVersion = :dataCloudVersion",
                getEntityClass().getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("amColumnId", amColumnId);
        query.setParameter("dataCloudVersion", dataCloudVersion);
        return (AccountMasterColumn) query.uniqueResult();

    }

}
