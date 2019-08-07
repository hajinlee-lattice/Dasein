package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.ConvertBatchStoreInfoDao;
import com.latticeengines.apps.cdl.entitymgr.ConvertBatchStoreInfoEntityMgr;
import com.latticeengines.apps.cdl.repository.ConvertBatchStoreInfoRepository;
import com.latticeengines.apps.cdl.repository.reader.ConvertBatchStoreInfoReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.ConvertBatchStoreInfoWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;

@Component("convertBatchStoreInfoEntityMgr")
public class ConvertBatchStoreInfoEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<ConvertBatchStoreInfoRepository, ConvertBatchStoreInfo, Long>
        implements ConvertBatchStoreInfoEntityMgr {

    @Inject
    private ConvertBatchStoreInfoEntityMgrImpl _self;

    @Inject
    private ConvertBatchStoreInfoDao convertBatchStoreInfoDao;

    @Inject
    private ConvertBatchStoreInfoReaderRepository readerRepository;

    @Inject
    private ConvertBatchStoreInfoWriterRepository writerRepository;

    @Override
    protected ConvertBatchStoreInfoRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected ConvertBatchStoreInfoRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<ConvertBatchStoreInfoRepository, ConvertBatchStoreInfo, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<ConvertBatchStoreInfo> getDao() {
        return convertBatchStoreInfoDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ConvertBatchStoreInfo findByPid(Long pid) {
        if (isReaderConnection()) {
            return readerRepository.findByPid(pid);
        } else {
            return writerRepository.findByPid(pid);
        }
    }
}
