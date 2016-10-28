package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

public class DnbLookupActor extends DataSourceWrapperActorTemplate {

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return new DnBLookupServiceImpl();
    }

}
