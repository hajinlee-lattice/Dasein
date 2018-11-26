package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

/**
 * Wrapper actor for {@link CDLAssociateServiceImpl}
 */
@Component("cdlAssociateActor")
@Scope("prototype")
public class CDLAssociateActor extends DataSourceWrapperActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(CDLAssociateActor.class);

    @Inject
    @Qualifier("cdlAssociateService")
    private DataSourceLookupServiceBase dnBCacheLookupService;

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dnBCacheLookupService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
