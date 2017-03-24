package com.latticeengines.pls.dao.impl;

import java.sql.Timestamp;
import java.util.List;

import javax.persistence.Table;

import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.type.StringType;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import com.latticeengines.pls.dao.ModelSummaryDownloadFlagDao;

@Component("modelSummaryDownloadFlagDao")
public class ModelSummaryDownloadFlagDaoImpl extends BaseDaoImpl<ModelSummaryDownloadFlag> implements ModelSummaryDownloadFlagDao {
    @Override
    protected Class<ModelSummaryDownloadFlag> getEntityClass() {
        return ModelSummaryDownloadFlag.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getWaitingFlags() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummaryDownloadFlag> entityClz = getEntityClass();
        String msFlagTable = entityClz.getAnnotation(Table.class).name();
        String sqlStr = String.format("SELECT msFlag.Tenant_ID FROM %s as msFlag GROUP BY msFlag.Tenant_ID",
                msFlagTable);
        SQLQuery sqlQuery = session.createSQLQuery(sqlStr).addScalar("Tenant_ID", new StringType());
        List<String> list = sqlQuery.list();
        if (list.size() == 0) {
            return null;
        } else {
            return list;
        }
    }

    @Override
    public void deleteOldFlags(long timeTicks) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummaryDownloadFlag> entityClz = getEntityClass();
        Timestamp timeLimit = new Timestamp(timeTicks);
        String queryStr = String.format("delete from %s where MARK_TIME < :timeLimit",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("timeLimit", timeLimit);
        query.executeUpdate();
    }
}
