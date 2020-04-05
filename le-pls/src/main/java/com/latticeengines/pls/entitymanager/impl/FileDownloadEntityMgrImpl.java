package com.latticeengines.pls.entitymanager.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.FileDownload;
import com.latticeengines.pls.dao.FileDownloadDao;
import com.latticeengines.pls.entitymanager.FileDownloadEntityMgr;
import com.latticeengines.pls.repository.reader.FileDownloadReaderRepository;

@Component("fileDownloadEntityMgr")
public class FileDownloadEntityMgrImpl extends BaseEntityMgrRepositoryImpl<FileDownload, Long> implements FileDownloadEntityMgr {

    @Inject
    private FileDownloadDao fileDownloadDao;

    @Inject
    private FileDownloadReaderRepository fileDownloadReaderRepository;


    @Override
    public BaseJpaRepository<FileDownload, Long> getRepository() {
        return fileDownloadReaderRepository;
    }

    @Override
    public BaseDao<FileDownload> getDao() {
        return fileDownloadDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public FileDownload findByToken(String token) {
        return fileDownloadReaderRepository.findByToken(token);
    }
}
