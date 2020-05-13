package com.latticeengines.app.exposed.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.dao.FileDownloadDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.FileDownload;

@Component("fileDownloadDao")
public class FileDownloadDaoImpl extends BaseDaoImpl<FileDownload> implements FileDownloadDao {

    @Override
    protected Class<FileDownload> getEntityClass() {
        return FileDownload.class;
    }
}
