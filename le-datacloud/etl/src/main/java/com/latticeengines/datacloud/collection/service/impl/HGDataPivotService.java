package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.PivotService;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.impl.HGDataPivoted;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("hgDataPivotService")
public class HGDataPivotService extends AbstractPivotService implements PivotService {

    @Inject
    private RefreshProgressEntityMgr progressEntityMgr;

    @Inject
    private HGDataPivoted source;

    @Override
    public String getBeanName() {
        return "hgDataPivotService";
    }

    @Override
    public PivotedSource getSource() { return source; }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executePivotData(
                source,
                progress.getBaseSourceVersion(),
                progress.getRootOperationUID(),
                "hgDataPivotFlow"
        );
    }


}
