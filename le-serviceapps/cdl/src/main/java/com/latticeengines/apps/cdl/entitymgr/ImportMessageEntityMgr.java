package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.ImportMessage;

public interface ImportMessageEntityMgr extends BaseEntityMgrRepository<ImportMessage, Long> {

    ImportMessage getBySourceId(String filePath);

    ImportMessage createOrUpdateImportMessage(ImportMessage importMessage);

}
