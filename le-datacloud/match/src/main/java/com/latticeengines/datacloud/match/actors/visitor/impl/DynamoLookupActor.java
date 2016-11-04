package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

@Component("dynamoLookupActor")
@Scope("prototype")
public class DynamoLookupActor extends DataSourceWrapperActorTemplate {
    private static final Log log = LogFactory.getLog(DynamoLookupActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

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
