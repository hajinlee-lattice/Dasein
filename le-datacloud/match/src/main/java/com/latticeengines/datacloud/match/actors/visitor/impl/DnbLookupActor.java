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

@Component("dnbLookupActor")
@Scope("prototype")
public class DnbLookupActor extends DataSourceWrapperActorTemplate {
    private static final Log log = LogFactory.getLog(DnbLookupActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Autowired
    @Qualifier("dnBLookupService")
    private DataSourceLookupServiceBase dnBLookupService;

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dnBLookupService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
