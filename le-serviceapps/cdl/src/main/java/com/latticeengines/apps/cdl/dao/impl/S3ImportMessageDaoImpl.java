package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.S3ImportMessageDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

@Component("s3ImportMessageDao")
public class S3ImportMessageDaoImpl extends BaseDaoImpl<S3ImportMessage> implements S3ImportMessageDao {
    @Override
    protected Class<S3ImportMessage> getEntityClass() {
        return S3ImportMessage.class;
    }
}
