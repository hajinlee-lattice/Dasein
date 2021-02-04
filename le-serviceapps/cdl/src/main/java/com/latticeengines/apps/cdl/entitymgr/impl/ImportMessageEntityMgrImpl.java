package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.entitymgr.ImportMessageEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.ImportMessageWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.ImportMessageReaderRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

@Component("importMessageEntityMgr")
public class ImportMessageEntityMgrImpl extends JpaEntityMgrRepositoryImpl<ImportMessage, Long> implements ImportMessageEntityMgr {

    @Inject
    private ImportMessageReaderRepository readerRepository;

    @Inject
    private ImportMessageWriterRepository writerRepository;

    @Override
    public BaseJpaRepository<ImportMessage, Long> getRepository() {
        return writerRepository;
    }


    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ImportMessage getBySourceId(String sourceId) {
        return readerRepository.findBySourceId(sourceId);
    }

    @Override
    @Transactional(transactionManager = "jpaTransactionManager", propagation = Propagation.REQUIRED)
    public ImportMessage createOrUpdateImportMessage(ImportMessage importMessage) {
        String sourceId = importMessage.getSourceId();
        ImportMessage message = writerRepository.findBySourceId(sourceId);
        if (message != null) {
            // update method may be used by other message type later
            if (StringUtils.isNotEmpty(importMessage.getKey())) {
                message.setKey(importMessage.getKey());
            }
            save(message);
            return message;
        } else {
            String bucket = importMessage.getBucket();
            String key = importMessage.getKey();
            S3ImportMessageType messageType = importMessage.getMessageType();
            message = new ImportMessage();
            message.setBucket(bucket);
            message.setKey(key);
            message.setSourceId(sourceId);
            message.setMessageType(messageType);
            save(message);
        }
        return message;
    }

}
