package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

public class DynamoLookupActor extends DataSourceWrapperActorTemplate {

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return new DynamoDBLookupServiceImpl();
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
