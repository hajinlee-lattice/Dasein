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

@Component("dunsGuideBookLookupActor")
@Scope("prototype")
public class DunsGuideBookLookupActor extends DataSourceWrapperActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(DunsGuideBookLookupActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Autowired
    @Qualifier("dunsGuideBookLookupService")
    private DataSourceLookupService dataSourceLookupService;

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dataSourceLookupService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
