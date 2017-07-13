package com.latticeengines.datacloud.collection.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.BulkArchiveService;
import com.latticeengines.datacloud.core.source.BulkSource;
import com.latticeengines.datacloud.core.source.impl.HGDataRaw;

@Component("hgDataRawArchiveService")
public class HGDataRawArchiveService extends AbstractBulkArchiveService implements BulkArchiveService {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Autowired
    HGDataRaw source;

    @Override
    public String getBeanName() {
        return "hgDataRawArchiveService";
    }

    @Override
    public BulkSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Logger getLogger() { return log; }

    @Override
    String getSrcTableSplitColumn() { return getSource().getDownloadSplitColumn(); }

}
