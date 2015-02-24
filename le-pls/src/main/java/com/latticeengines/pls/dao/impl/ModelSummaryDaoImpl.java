package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.pls.dao.ModelSummaryDao;


public class ModelSummaryDaoImpl extends BaseDaoImpl<ModelSummary> implements ModelSummaryDao {

    @Override
    protected Class<ModelSummary> getEntityClass() {
        return ModelSummary.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ModelSummary findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where id = :modelId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }


    @SuppressWarnings("rawtypes")
    @Override
    public ModelSummary findByModelName(String modelName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :modelName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelName", modelName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public List<ModelSummary> findAllValid() {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        String queryStr = String.format("from %s where status != :statusId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        List<ModelSummary> list = query.list();
        return list;
    }

    @Override
    public ModelSummary findValidByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelSummary> entityClz = getEntityClass();
        Query query = session.createQuery("from " + entityClz.getSimpleName() + " where id = :modelId AND STATUS != :statusId");
        query.setString("modelId", modelId);
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (ModelSummary) list.get(0);
    }

}
