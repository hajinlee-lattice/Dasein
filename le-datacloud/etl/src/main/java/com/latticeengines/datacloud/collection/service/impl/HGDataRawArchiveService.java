package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.BulkArchiveService;
import com.latticeengines.datacloud.core.source.BulkSource;
import com.latticeengines.datacloud.core.source.impl.HGDataRaw;

@Component("hgDataRawArchiveService")
public class HGDataRawArchiveService extends AbstractBulkArchiveService implements BulkArchiveService {

    @Inject
    private ArchiveProgressEntityMgr progressEntityMgr;

    @Inject
    private HGDataRaw source;

    @Override
    public String getBeanName() {
        return "hgDataRawArchiveService";
    }

    @Override
    public BulkSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    String getSrcTableSplitColumn() { return getSource().getDownloadSplitColumn(); }

}
