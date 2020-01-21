package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

/**
 * Wrapper actor for {@link EntityFetchServiceImpl}
 */
@Component("entityFetchActor")
@Scope("prototype")
public class EntityFetchActor extends DataSourceWrapperActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(EntityFetchActor.class);

    @Autowired
    @Qualifier("entityFetchService")
    private DataSourceLookupServiceBase entityFetchService;

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return entityFetchService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
