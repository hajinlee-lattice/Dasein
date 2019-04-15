package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.S3ImportSystemDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

@Component("s3ImportSystemDao")
public class S3ImportSystemDaoImpl extends BaseDaoImpl<S3ImportSystem> implements S3ImportSystemDao {
    @Override
    protected Class<S3ImportSystem> getEntityClass() {
        return S3ImportSystem.class;
    }
}
