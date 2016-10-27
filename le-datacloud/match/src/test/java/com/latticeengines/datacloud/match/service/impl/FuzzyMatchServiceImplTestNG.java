//package com.latticeengines.datacloud.match.service.impl;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//
//import org.junit.Assert;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.testng.annotations.Test;
//
//import com.latticeengines.datacloud.match.actors.visitor.MatchActorSystemWrapper;
//import com.latticeengines.datacloud.match.service.FuzzyMatchService;
//import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
//
//@Test
//public class FuzzyMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {
//    @Autowired
//    private FuzzyMatchService service;
//
//    @Autowired
//    private MatchActorSystemWrapper wrapper;
//
//    @Test
//    public void test() throws Exception {
//        try {
//            List<Object> matchRequests = new ArrayList<>();
//            for (int i = 0; i < 2; i++) {
//                Object msg = UUID.randomUUID().toString();
//                matchRequests.add(msg);
//            }
//
//            int idx = 0;
//            for (Object result : service.callMatch(matchRequests)) {
//                System.out.println(result);
//                Assert.assertNotNull(result);
//                Assert.assertNotEquals(result, matchRequests.get(idx++));
//            }
//        } finally {
//            wrapper.shutdown();
//        }
//    }
//}
