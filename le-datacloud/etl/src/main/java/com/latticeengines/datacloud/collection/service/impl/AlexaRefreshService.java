package com.latticeengines.datacloud.collection.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.impl.AlexaMostRecent;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("alexaRefreshService")
public class AlexaRefreshService extends AbstractMostRecentService implements RefreshService {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Autowired
    AlexaMostRecent source;

    @Override
    public String getBeanName() {
        return "alexaRefreshService";
    }

    @Override
    public MostRecentSource getSource() {
        return source;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executeMergeRawData(source, progress.getRootOperationUID(), "alexaRefreshFlow");
    }
}
