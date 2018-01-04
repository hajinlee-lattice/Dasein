package com.latticeengines.modelquality.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public abstract class ModelQualityBaseDaoImpl<T extends HasPid> extends BaseDaoImpl<T> {

    @Override
    public void delete(T entity) {
        if(entity == null) {
            return;
        }
        Session session = getSessionFactory().getCurrentSession();
        Query query = session.createQuery("delete from " + getEntityClass().getSimpleName() + " where pid = :pid").setParameter("pid", entity.getPid());
        query.executeUpdate();
    }
}
