//package com.latticeengines.actors.visitor.sample.impl;
//
//import java.util.Map;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.latticeengines.actors.exposed.traveler.TravelContext;
//import com.latticeengines.actors.visitor.sample.SampleMicroEngineActorTemplate;
//
//public class SampleDomainBasedMicroEngineActor extends SampleMicroEngineActorTemplate {
//    private static final Log log = LogFactory.getLog(SampleDomainBasedMicroEngineActor.class);
//
//    @Override
//    protected Log getLogger() {
//        return log;
//    }
//
//    @Override
//    protected String getDataSourceActor() {
//        return "dynamo";
//    }
//
//    @Override
//    protected boolean accept(TravelContext traveler) {
//        Map<String, Object> dataKeyValueMap = traveler.getDataKeyValueMap();
//
//        if (dataKeyValueMap.containsKey("Domain")) {
//            return true;
//        }
//
//        return false;
//    }
//
//}
