package com.latticeengines.actors.visitor.sample.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupService;
import com.latticeengines.actors.visitor.sample.SampleDataSourceWrapperActorTemplate;

@Component("sampleDnbLookupActor")
@Scope("prototype")
public class SampleDnbLookupActor extends SampleDataSourceWrapperActorTemplate {

    @Autowired
    @Qualifier("sampleDnBLookupService")
    private SampleDataSourceLookupService dnBLookupService;

    @Override
    protected SampleDataSourceLookupService getDataSourceLookupService() {
        return dnBLookupService;
    }
}
