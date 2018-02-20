package com.latticeengines.documentdb.entitymgr.impl;

import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;

public abstract class BaseDocumentEntityMgrImpl<T> extends JpaEntityMgrRepositoryImpl<T, String> {

    @Override
    public void save(T entity) {
        super.save(entity);
    }

}
