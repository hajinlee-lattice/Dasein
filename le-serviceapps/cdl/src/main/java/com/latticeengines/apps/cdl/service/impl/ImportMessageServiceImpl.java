package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ImportMessageEntityMgr;
import com.latticeengines.apps.cdl.service.ImportMessageService;
import com.latticeengines.domain.exposed.cdl.ImportMessage;

@Component("importMessageService")
public class ImportMessageServiceImpl implements ImportMessageService {

    @Inject
    private ImportMessageEntityMgr importMessageEntityMgr;

    @Override
    public ImportMessage getBySourceId(String sourceId) {
        return importMessageEntityMgr.getBySourceId(sourceId);
    }

    @Override
    public ImportMessage createOrUpdateImportMessage(ImportMessage importMessage) {
        return importMessageEntityMgr.createOrUpdateImportMessage(importMessage);
    }
}
