package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.DataOperationDao;
import com.latticeengines.apps.cdl.entitymgr.DataOperationEntityMgr;
import com.latticeengines.apps.cdl.repository.DataOperationRepository;
import com.latticeengines.apps.cdl.repository.reader.DataOperationReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.DataOperationWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataOperation;

@Component("dataOperationEntityMgr")
public class DataOperationEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<DataOperationRepository, DataOperation, Long>
        implements DataOperationEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(DataOperationEntityMgrImpl.class);

    @Inject
    private DataOperationEntityMgrImpl _self;

    @Inject
    private DataOperationDao dataOperationDao;

    @Inject
    private DataOperationReaderRepository dataOperationReaderRepository;

    @Inject
    private DataOperationWriterRepository dataOperationWriterRepository;

    @Override
    protected DataOperationRepository getReaderRepo() {
        return dataOperationReaderRepository;
    }

    @Override
    protected DataOperationRepository getWriterRepo() {
        return dataOperationWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<DataOperationRepository, DataOperation, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<DataOperation> getDao() {
        return dataOperationDao;
    }

}
