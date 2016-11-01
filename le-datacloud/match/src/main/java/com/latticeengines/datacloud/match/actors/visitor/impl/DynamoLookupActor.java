package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

@Component("dynamoLookupActor")
@Scope("prototype")
public class DynamoLookupActor extends DataSourceWrapperActorTemplate {
    @Autowired
    @Qualifier("dynamoDBLookupService")
    private DataSourceLookupService dynamoDBLookupService;

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dynamoDBLookupService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
