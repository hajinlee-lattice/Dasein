package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectedArchiveService;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.impl.Feature;

@Component("featureArchiveService")
public class FeatureArchiveService extends AbstractCollectionArchiveService implements CollectedArchiveService {

    @Inject
    private ArchiveProgressEntityMgr progressEntityMgr;

    @Inject
    private Feature source;

    @Override
    public String getBeanName() {
        return "featureArchiveService";
    }

    @Override
    public CollectedSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

}
