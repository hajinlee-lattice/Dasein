package com.latticeengines.app.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.FileDownload;

public interface FileDownloadEntityMgr extends BaseEntityMgrRepository<FileDownload, Long> {

    FileDownload getByToken(String token);

}
