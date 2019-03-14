package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.S3ImportMessageDao;
import com.latticeengines.apps.cdl.entitymgr.S3ImportMessageEntityMgr;
import com.latticeengines.apps.cdl.repository.S3ImportMessageRepository;
import com.latticeengines.apps.cdl.repository.reader.S3ImportMessageReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.S3ImportMessageWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

@Component("s3ImportMessageEntityMgr")
public class S3ImportMessageEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<S3ImportMessageRepository, S3ImportMessage, Long>
        implements S3ImportMessageEntityMgr {

    @Inject
    private S3ImportMessageEntityMgrImpl _self;

    @Inject
    private S3ImportMessageDao s3ImportMessageDao;

    @Inject
    private S3ImportMessageReaderRepository readerRepository;

    @Inject
    private S3ImportMessageWriterRepository writerRepository;


    @Override
    protected S3ImportMessageRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected S3ImportMessageRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected S3ImportMessageEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    public BaseDao<S3ImportMessage> getDao() {
        return s3ImportMessageDao;
    }

    @Override
    public S3ImportMessage createS3ImportMessage(String bucket, String key) {
        return null;
    }
}
