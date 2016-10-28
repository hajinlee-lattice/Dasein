package com.latticeengines.actors.visitor.sample.impl;

import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupService;
import com.latticeengines.actors.visitor.sample.SampleDataSourceWrapperActorTemplate;

public class SampleDnbLookupActor extends SampleDataSourceWrapperActorTemplate {

    @Override
    protected SampleDataSourceLookupService getDataSourceLookupService() {
        return new SampleDnBLookupServiceImpl();
    }

}
