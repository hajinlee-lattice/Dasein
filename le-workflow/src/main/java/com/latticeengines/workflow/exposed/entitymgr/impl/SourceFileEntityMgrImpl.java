package com.latticeengines.workflow.exposed.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.workflow.exposed.dao.SourceFileDao;
import com.latticeengines.workflow.exposed.entitymgr.SourceFileEntityMgr;

@Component("sourceFileEntityMgr")
public class SourceFileEntityMgrImpl extends BaseEntityMgrImpl<SourceFile> implements SourceFileEntityMgr {

    @Autowired
    private SourceFileDao sourceFileDao;

    @Override
    public BaseDao<SourceFile> getDao() {
        return sourceFileDao;
    }

    @Override
    public SourceFile findByKey(Class<SourceFile> entityClz, Long key) {
        return null;
    }

    @Override
    public <F> SourceFile findByField(String fieldName, F value) {
        return null;
    }

}
