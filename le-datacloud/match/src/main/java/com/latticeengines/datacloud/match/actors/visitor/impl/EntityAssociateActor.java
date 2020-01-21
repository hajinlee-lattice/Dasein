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
 * Wrapper actor for {@link EntityAssociateServiceImpl}
 */
@Component("entityAssociateActor")
@Scope("prototype")
public class EntityAssociateActor extends DataSourceWrapperActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(EntityAssociateActor.class);

    @Autowired
    @Qualifier("entityAssociateService")
    private DataSourceLookupServiceBase entityAssociateService;

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return entityAssociateService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
