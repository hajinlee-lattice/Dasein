package com.latticeengines.pls.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import com.latticeengines.pls.dao.ModelSummaryDownloadFlagDao;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("modelSummaryDownloadFlagDao")
public class ModelSummaryDownloadFlagDaoImpl extends BaseDaoImpl<ModelSummaryDownloadFlag> implements ModelSummaryDownloadFlagDao {
    @Override
    protected Class<ModelSummaryDownloadFlag> getEntityClass() {
        return ModelSummaryDownloadFlag.class;
    }

    @Override
    public List<ModelSummaryDownloadFlag> getAllFlags() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummaryDownloadFlag> entityClz = getEntityClass();
        String queryStr = String.format("from %s", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            List<ModelSummaryDownloadFlag> allFlags = new ArrayList<>();
            for (int i = 0; i < list.size(); i++) {
                allFlags.add((ModelSummaryDownloadFlag) list.get(i));
            }
            return allFlags;
        }
    }

    @Override
    public List<ModelSummaryDownloadFlag> getDownloadedFlags() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummaryDownloadFlag> entityClz = getEntityClass();
        String queryStr = String.format("from %s where DOWNLOADED = :downloaded order by PID",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setBoolean("downloaded", true);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            List<ModelSummaryDownloadFlag> allFlags = new ArrayList<>();
            for (int i = 0; i < list.size(); i++) {
                allFlags.add((ModelSummaryDownloadFlag) list.get(i));
            }
            return allFlags;
        }
    }

    @Override
    public List<ModelSummaryDownloadFlag> getWaitingFlags() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummaryDownloadFlag> entityClz = getEntityClass();
        String queryStr = String.format("from %s where DOWNLOADED = :downloaded order by PID",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setBoolean("downloaded", false);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            List<ModelSummaryDownloadFlag> allFlags = new ArrayList<>();
            for (int i = 0; i < list.size(); i++) {
                allFlags.add((ModelSummaryDownloadFlag) list.get(i));
            }
            return allFlags;
        }
    }
}
