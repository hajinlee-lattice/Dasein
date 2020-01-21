package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.impl.BuiltWithMostRecent;


@Component("builtWithRefreshService")
public class BuiltWithRefreshService extends AbstractMostRecentService implements RefreshService {

    @Inject
    private RefreshProgressEntityMgr progressEntityMgr;

    @Inject
    private BuiltWithMostRecent source;

    @Override
    public String getBeanName() {
        return "builtWithRefreshService";
    }

    @Override
    public MostRecentSource getSource() {
        return source;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

}
