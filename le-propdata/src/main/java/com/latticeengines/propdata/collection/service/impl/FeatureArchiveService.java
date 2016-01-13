package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.impl.Feature;

@Component("featureArchiveService")
public class FeatureArchiveService extends AbstractCollectionArchiveService implements CollectedArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Autowired
    Feature source;

    @Override
    public CollectedSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }
}
