//package com.latticeengines.sampleapi.sample.service.impl;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//
//import org.junit.Assert;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.test.context.ContextConfiguration;
//import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
//import org.testng.annotations.Test;
//
//import com.latticeengines.actors.visitor.sample.SampleMatchActorSystemWrapper;
//import com.latticeengines.sampleapi.sample.service.SampleService;
//
//@Test
//@ContextConfiguration(locations = { "classpath:test-sample-service-context.xml" })
//public class SampleServiceImplUnitTestNG extends AbstractTestNGSpringContextTests {
//    @Autowired
//    private SampleMatchActorSystemWrapper wrapper;
//
//    @Autowired
//    private SampleService service;
//
//    @Test(groups = { "unit" })
//    public void test() throws Exception {
//        try {
//            List<Map<String, Object>> matchRequests = new ArrayList<>();
//            int MAX = 2;
//            for (int i = 0; i < MAX; i++) {
//                Map<String, Object> dataKeyValueMap = new HashMap<>();
//                dataKeyValueMap.put("Domain", UUID.randomUUID().toString());
//                if (i % 2 != 1) {
//                    dataKeyValueMap.put("DUNS", UUID.randomUUID().toString());
//                }
//                dataKeyValueMap.put("CompanyName", UUID.randomUUID().toString());
//                dataKeyValueMap.put("Country", UUID.randomUUID().toString());
//                dataKeyValueMap.put("State", UUID.randomUUID().toString());
//
//                matchRequests.add(dataKeyValueMap);
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
