package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.BulkArchiveService;
import com.latticeengines.propdata.collection.source.BulkSource;
import com.latticeengines.propdata.collection.source.impl.HGDataRaw;

@Component("hgDataRawArchiveService")
public class HGDataRawArchiveService extends AbstractBulkArchiveService implements BulkArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Autowired
    HGDataRaw source;

    @Override
    public BulkSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    String getSrcTableSplitColumn() { return getSource().getDownloadSplitColumn(); }

}
