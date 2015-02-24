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
        Query query = session.createQuery("from " + entityClz.getSimpleName() + " where id = :modelId");
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
        Query query = session.createQuery("from " + entityClz.getSimpleName() + " where name = :modelName");
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
        Query query = session.createQuery("from " + entityClz.getSimpleName() + " where STATUS != :statusId");
        query.setInteger("statusId", ModelSummaryStatus.DELETED.getStatusId());
        List<ModelSummary> list = query.list();
        return list;
    }

}
