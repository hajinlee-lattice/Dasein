package com.latticeengines.actors.visitor.sample.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupService;
import com.latticeengines.actors.visitor.sample.SampleDataSourceWrapperActorTemplate;

@Component("sampleDynamoLookupActor")
@Scope("prototype")
public class SampleDynamoLookupActor extends SampleDataSourceWrapperActorTemplate {

    @Autowired
    @Qualifier("sampleDynamoDBLookupService")
    private SampleDataSourceLookupService dynamoDBLookupService;

    @Override
    protected SampleDataSourceLookupService getDataSourceLookupService() {
        return dynamoDBLookupService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
