package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;

@Component("dnbLookupActor")
@Scope("prototype")
public class DnbLookupActor extends DataSourceWrapperActorTemplate {

    @Autowired
    @Qualifier("dnBLookupService")
    private DataSourceLookupService dnBLookupService;

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dnBLookupService;
    }
}
