package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.UploadDao;
import com.latticeengines.apps.dcp.entitymgr.UploadEntityMgr;
import com.latticeengines.apps.dcp.repository.UploadRepository;
import com.latticeengines.apps.dcp.repository.reader.UploadReaderRepository;
import com.latticeengines.apps.dcp.repository.writer.UploadWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.Upload;

@Component("uploadEntiyMgr")
public class UploadEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<UploadRepository, Upload, Long>
        implements UploadEntityMgr {

    @Inject
    private UploadEntityMgrImpl _self;

    @Inject
    private UploadDao uploadDao;

    @Inject
    private UploadReaderRepository uploadReaderRepository;

    @Inject
    private UploadWriterRepository uploadWriterRepository;

    @Override
    protected UploadRepository getReaderRepo() {
        return uploadReaderRepository;
    }

    @Override
    protected UploadRepository getWriterRepo() {
        return uploadWriterRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<UploadRepository, Upload, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<Upload> getDao() {
        return uploadDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Upload> findBySourceId(String sourceId) {
        return getReadOrWriteRepository().findBySourceId(sourceId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Upload> findBySourceIdAndStatus(String sourceId, Upload.Status status) {
        return getReadOrWriteRepository().findBySourceIdAndStatus(sourceId, status);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Upload findByUploadId(String uploadId) {
        return getReadOrWriteRepository().findByUploadId(uploadId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String findMatchResultTableNameByUploadId(String uploadId) {
        return getReadOrWriteRepository().findMatchResultTableNameByUploadId(uploadId);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String findMatchCandidatesTableNameByUploadId(String uploadId) {
        return getReadOrWriteRepository().findMatchCandidatesTableNameByUploadId(uploadId);
    }

}
