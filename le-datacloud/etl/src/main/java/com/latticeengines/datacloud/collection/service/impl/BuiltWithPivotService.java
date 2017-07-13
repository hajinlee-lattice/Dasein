package com.latticeengines.datacloud.collection.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.PivotService;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.impl.BuiltWithPivoted;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;

@Component("builtWithPivotService")
public class BuiltWithPivotService extends AbstractPivotService implements PivotService {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Autowired
    BuiltWithPivoted source;

    @Override
    public String getBeanName() {
        return "builtWithPivotService";
    }

    @Override
    public PivotedSource getSource() { return source; }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Logger getLogger() { return log; }

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executePivotData(
                source,
                progress.getBaseSourceVersion(),
                progress.getRootOperationUID(),
                "builtWithPivotFlow"
        );
    }


}
