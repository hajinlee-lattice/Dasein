package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.S3ImportMessageDao;
import com.latticeengines.apps.cdl.entitymgr.S3ImportMessageEntityMgr;
import com.latticeengines.apps.cdl.repository.S3ImportMessageRepository;
import com.latticeengines.apps.cdl.repository.reader.S3ImportMessageReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.S3ImportMessageWriterRepository;
import com.latticeengines.apps.core.entitymgr.DropBoxEntityMgr;
import com.latticeengines.apps.core.util.S3ImportMessageUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

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

    @Inject
    private DropBoxEntityMgr dropBoxEntityMgr;

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
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public S3ImportMessage createOrUpdateS3ImportMessage(String bucket, String key, S3ImportMessageType messageType) {
        S3ImportMessage message = getReadOrWriteRepository().findByKey(key);
        if (message != null) {
            s3ImportMessageDao.update(message);
            return message;
        } else {
            message = new S3ImportMessage();
            message.setBucket(bucket);
            message.setKey(key);
            if (S3ImportMessageType.Atlas.equals(messageType)) {
                message.setFeedType(S3ImportMessageUtils.getFeedTypeFromKey(key));
            }
            if( messageType != S3ImportMessageType.LISTSEGMENT) {
                String dropBoxPrefix = S3ImportMessageUtils.getDropBoxPrefix(key);
                DropBox dropBox = dropBoxEntityMgr.getDropBox(dropBoxPrefix);
                message.setDropBox(dropBox);
            }
            message.setMessageType(messageType);
            s3ImportMessageDao.create(message);
        }
        return message;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<S3ImportMessage> getS3ImportMessageGroupByDropBox() {
        return getReadOrWriteRepository().getS3ImportMessageGroupByDropBox();
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public void updateHostUrl(String key, String hostUrl) {
        writerRepository.updateHostUrl(key, hostUrl);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<S3ImportMessage> getMessageWithoutHostUrlByType(S3ImportMessageType messageType) {
        return getReadOrWriteRepository().findByMessageTypeAndHostUrlIsNull(messageType);
    }
}
