package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.FileDownload;

public interface FileDownloadEntityMgr extends BaseEntityMgrRepository<FileDownload, Long> {

    FileDownload findByToken(String token);

}
