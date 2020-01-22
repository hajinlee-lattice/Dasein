package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

@Component("dnbLookupActor")
@Scope("prototype")
public class DnbLookupActor extends DataSourceWrapperActorTemplate {
    private static final Logger log = LoggerFactory.getLogger(DnbLookupActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Resource(name = "dnbLookupService")
    private DataSourceLookupServiceBase dnbLookupService;

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dnbLookupService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
