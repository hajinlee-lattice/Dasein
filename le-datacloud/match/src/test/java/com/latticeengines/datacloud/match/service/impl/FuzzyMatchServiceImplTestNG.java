package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

@Test
public class FuzzyMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private FuzzyMatchService service;

    @Test(groups = "pending")
    public void testActorSystem() throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        try {
            List<Map<String, Object>> matchRequests = new ArrayList<>();
            int MAX = 50;
            for (int i = 0; i < MAX; i++) {
                if (i == 0) {
                    continue;
                }
                Map<String, Object> dataKeyValueMap = new HashMap<>();
                dataKeyValueMap.put("Domain", UUID.randomUUID().toString());
                if (i % 2 != 1) {
                    dataKeyValueMap.put("DUNS", UUID.randomUUID().toString());
                }
                dataKeyValueMap.put("CompanyName", UUID.randomUUID().toString());
                dataKeyValueMap.put("Country", UUID.randomUUID().toString());
                dataKeyValueMap.put("State", UUID.randomUUID().toString());

                matchRequests.add(dataKeyValueMap);
            }

            int idx = 0;
            for (Object result : service.callMatch(matchRequests, "2.0.0")) {
                System.out.println(result);
                Assert.assertNotNull(result);
                Assert.assertNotEquals(result, matchRequests.get(idx++));
            }
        } finally {
            LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.INFO);
            LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.INFO);
        }
    }
}
