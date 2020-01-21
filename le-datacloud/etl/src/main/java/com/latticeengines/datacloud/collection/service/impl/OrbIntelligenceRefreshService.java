package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.impl.OrbIntelligenceMostRecent;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("orbIntelligenceRefreshService")
public class OrbIntelligenceRefreshService extends AbstractMostRecentService implements RefreshService {

    @Inject
    private RefreshProgressEntityMgr progressEntityMgr;

    @Inject
    private OrbIntelligenceMostRecent source;

    @Override
    public String getBeanName() {
        return "orbIntelligenceRefreshService";
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
        collectionDataFlowService.executeMergeRawData(source, progress.getRootOperationUID(),
                "orbIntelligenceRefreshFlow");
    }
}
