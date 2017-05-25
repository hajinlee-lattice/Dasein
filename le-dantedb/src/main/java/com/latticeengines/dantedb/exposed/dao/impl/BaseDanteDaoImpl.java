package com.latticeengines.dantedb.exposed.dao.impl;

import java.util.Date;

import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.dantetalkingpoints.HasDanteAuditingFields;

public abstract class BaseDanteDaoImpl<T extends HasDanteAuditingFields>
        extends BaseDaoWithAssignedSessionFactoryImpl<T> implements BaseDanteDao<T> {

    public T findByExternalID(String externalID) {
        return findByField("External_ID", externalID);
    }

    @Override
    public void create(T entity) {
        setAuditingFields(entity);
        super.create(entity);
    }

    @Override
    public void createOrUpdate(T entity) {
        setAuditingFields(entity);
        super.createOrUpdate(entity);
    }

    @Override
    public void update(T entity) {
        setAuditingFields(entity);
        super.update(entity);
    }

    private void setAuditingFields(T entity) {
        if (entity != null) {
            Date now = new Date();
            if (entity.getPid() == null) {
                entity.setCreationDate(now);
            }
            entity.setLastModificationDate(now);
        }
    }
}
