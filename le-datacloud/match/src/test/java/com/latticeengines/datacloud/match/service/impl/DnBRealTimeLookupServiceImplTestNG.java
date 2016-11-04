package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.DnBRealTimeLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.DnBMatchEntry;
import com.latticeengines.domain.exposed.datacloud.match.DnBReturnCode;

public class DnBRealTimeLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(DnBRealTimeLookupServiceImplTestNG.class);

    @Autowired
    private DnBRealTimeLookupService dnBRealTimeLookupService;

    private final static int THREAD_NUM = 20;

    @Test(groups = "functional", dataProvider = "inputData", enabled = true)
    public void testRealTimeLookupService(String name, String city, String state, String country, String message) {
        DnBMatchEntry input = new DnBMatchEntry();
        input.setCountryCode(country);
        input.setName(name);
        input.setState(state);
        input.setCity(city);

        dnBRealTimeLookupService.realtimeEntityLookup(input);
        Assert.assertEquals(input.getMessages().get(0), message);
    }

    @Test(groups = "functional", enabled = false)
    public void loadTestRealTimeLookupService() {

        List<String> messageList = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUM);
        CompletionService<DnBMatchEntry> cs = new ExecutorCompletionService<DnBMatchEntry>(executorService);

        for (int i = 0; i < THREAD_NUM; i++) {
            cs.submit(new Callable<DnBMatchEntry>() {
                public DnBMatchEntry call() throws Exception {
                    DnBMatchEntry input = new DnBMatchEntry();
                    input.setCountryCode("US");
                    input.setName("Gorman Manufacturing");
                    input.setState("CA");

                    dnBRealTimeLookupService.realtimeEntityLookup(input);
                    return input;
                }
            });
            log.info("Submit :" + i);
        }

        for (int i = 0; i < THREAD_NUM; i++) {
            try {
                DnBMatchEntry result = cs.take().get();
                log.info(i + " Name: " + result.getName() + " message:" + result.getMessages());
                messageList.add(result.getMessages().get(0));
            } catch (InterruptedException e) {
                log.error(e);
            } catch (ExecutionException e) {
                log.error(e);
            }
        }

        for(int i = 0; i < THREAD_NUM; i++) {
            if(messageList.get(i).equals(DnBReturnCode.ExceedConcurrentNum.getMessage())) {
                Assert.assertTrue(true);
            }
        }
        Assert.assertTrue(false);
    }

    @DataProvider(name = "inputData")
    public static Object[][] getInputData() {
        return new Object[][] { { "Benchmark Blinds", "Gilbert", "Arizona", "US", DnBReturnCode.Ok.getMessage() },
                { "\"El Camino Machine & Welding, LLC\"", "Salinas", "California", "US",
                        DnBReturnCode.Ok.getMessage() },
                { "Cornerstone Alliance Church", "Canon City", "Colorado", "US", DnBReturnCode.Ok.getMessage() },
                { "  Gorman Manufacturing  ", "", "", "  US  ", DnBReturnCode.Ok.getMessage() } };
    }
}
