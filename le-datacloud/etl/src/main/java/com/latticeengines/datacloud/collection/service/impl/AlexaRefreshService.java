package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.impl.AlexaMostRecent;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("alexaRefreshService")
public class AlexaRefreshService extends AbstractMostRecentService implements RefreshService {

    @Inject
    private RefreshProgressEntityMgr progressEntityMgr;

    @Inject
    private AlexaMostRecent source;

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
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executeMergeRawData(source, progress.getRootOperationUID(), "alexaRefreshFlow");
    }
}
