package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.ImportMessage;

public interface ImportMessageService {

    ImportMessage getBySourceId(String filePath);

    ImportMessage createOrUpdateImportMessage(ImportMessage importMessage);

}
