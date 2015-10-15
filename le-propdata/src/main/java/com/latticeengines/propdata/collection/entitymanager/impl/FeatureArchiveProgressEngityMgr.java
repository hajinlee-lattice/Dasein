package com.latticeengines.propdata.collection.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;
import com.latticeengines.propdata.collection.dao.ArchiveProgressDao;
import com.latticeengines.propdata.collection.dao.FeatureArchiveProgressDao;
import com.latticeengines.propdata.collection.entitymanager.FeatureArchiveProgressEntityMgr;

@Component
public class FeatureArchiveProgressEngityMgr extends AbstractArchiveProgressEntityMgr<FeatureArchiveProgress>
        implements FeatureArchiveProgressEntityMgr {

    @Autowired
    FeatureArchiveProgressDao progressDao;

    @Override
    ArchiveProgressDao<FeatureArchiveProgress> getProgressDao() { return progressDao; }

    @Override
    public Class<FeatureArchiveProgress> getProgressClass() { return FeatureArchiveProgress.class; }

}

