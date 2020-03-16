package com.latticeengines.apps.dcp.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.dcp.dao.UploadDao;
import com.latticeengines.apps.dcp.entitymgr.UploadEntityMgr;
import com.latticeengines.apps.dcp.repository.UploadRepository;
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

    @Resource(name = "uploadReaderRepository")
    private UploadRepository uploadReaderRepository;

    @Resource(name = "uploadWriterRepository")
    private UploadRepository uploadWriterRepository;

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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Upload> findBySourceId(String sourceId) {
        if (isReaderConnection()) {
            return uploadReaderRepository.findBySourceId(sourceId);
        } else {
            return uploadWriterRepository.findBySourceId(sourceId);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Upload> findBySourceIdAndStatus(String sourceId, Upload.Status status) {
        if (isReaderConnection()) {
            return uploadReaderRepository.findBySourceIdAndStatus(sourceId, status);
        } else {
            return uploadWriterRepository.findBySourceIdAndStatus(sourceId, status);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Upload findByPid(Long pid) {
        if (isReaderConnection()) {
            return uploadReaderRepository.findByPid(pid);
        } else {
            return uploadWriterRepository.findByPid(pid);
        }
    }
}
