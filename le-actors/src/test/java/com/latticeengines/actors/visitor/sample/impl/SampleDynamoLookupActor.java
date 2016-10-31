//package com.latticeengines.actors.visitor.sample.impl;
//
//import com.latticeengines.actors.visitor.sample.SampleDataSourceLookupService;
//import com.latticeengines.actors.visitor.sample.SampleDataSourceWrapperActorTemplate;
//
//public class SampleDynamoLookupActor extends SampleDataSourceWrapperActorTemplate {
//
//    @Override
//    protected SampleDataSourceLookupService getDataSourceLookupService() {
//        return new SampleDynamoDBLookupServiceImpl();
//    }
//
//    @Override
//    protected boolean shouldDoAsyncLookup() {
//        return true;
//    }
//}
